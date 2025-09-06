// Package chunk is responsible for handling the chunking of a particular file into fixed size units.
package chunk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

const DefaultChunkSize = 1024 * 1024 // 1MB

// Chunker should be able to return a chunk meta of an object, which includes the indivulal chunk hashes, the overall hash, and the total size of the object.  Chunker should also be able to read an individual chunk by its hash.
type Chunker interface {
	Meta(path string) (*ChunkMeta, error)
	// TODO: are we using the hash to error if it doesn't match the data read?
	Data(path string, hash Hash, index int64) ([]byte, error)
	Merge(ctx context.Context, path string, hash Hash, totalChunks int64, ch <-chan ChunkData) error
}

var _ Chunker = (*chunker)(nil)

type chunker struct {
	width   int64
	baseDir string
}

func NewChunker(width int64, baseDir string) (Chunker, error) {
	if !filepath.IsAbs(baseDir) {
		return nil, ErrAbsolutePathRequired
	}

	return &chunker{
		width:   width,
		baseDir: baseDir,
	}, nil
}

func (c *chunker) Meta(path string) (meta *ChunkMeta, err error) {
	if filepath.IsAbs(path) {
		return nil, ErrRelativePathRequired
	}

	p := filepath.Join(c.baseDir, path)

	file, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)

	hashes, hash, size, err := c.chunkIt(reader)
	if err != nil {
		return nil, err
	}

	return meta.FromProto(&globv1proto.ChunkMeta{
		TotalBytes: size,
		Hashes:     hashes.Bytes(),
		Hash:       hash.Bytes(),
		ByteWidth:  DefaultChunkSize,
	}), nil
}

// chunkIt returns a slice of []byte representing list of hahes, the sum total hash
func (c *chunker) chunkIt(reader *bufio.Reader) (hashes Hashes, hash Hash, size int64, err error) {
	totalHasher := sha512.New()

	for {
		var (
			chunk     = make([]byte, c.width)
			hasher    = sha512.New()
			bytesRead int
		)

		bytesRead, err = io.ReadFull(reader, chunk)

		// We always process whatever we read, even if it's less than a full chunk.
		hasher.Write(chunk[:bytesRead])
		totalHasher.Write(chunk[:bytesRead])
		size += int64(bytesRead)

		if bytesRead > 0 {

			var h Hash
			err = h.FromBytes(hasher.Sum(nil))
			if err != nil {
				return nil, Hash{}, 0, err
			}

			hashes = append(hashes, h)
			hasher.Reset()
		}

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, Hash{}, 0, err
		}
	}

	var totalHash Hash
	err = totalHash.FromBytes(totalHasher.Sum(nil))
	if err != nil {
		return nil, Hash{}, 0, err
	}

	return hashes, totalHash, size, nil
}

func (c *chunker) Data(path string, hash Hash, index int64) ([]byte, error) {
	if filepath.IsAbs(path) {
		return nil, ErrRelativePathRequired
	}

	p := filepath.Join(c.baseDir, path)

	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	data := make([]byte, c.width)
	offset := index * c.width

	i, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("reading at %d: %w", offset, err)
	}

	// Hash only the bytes read
	var dataHash Hash = sha512.Sum512(data[:i])

	if !bytes.Equal(hash.Bytes(), dataHash.Bytes()) {
		return nil, ErrChecksumNotMatched
	}

	// Return only the bytes read
	return data[:i], nil
}

func (c *chunker) Merge(ctx context.Context, path string, totalHash Hash, totalChunks int64, chunks <-chan ChunkData) error {
	if filepath.IsAbs(path) {
		return ErrRelativePathRequired
	}

	p := filepath.Join(c.baseDir, path)

	tempFile, err := os.CreateTemp(c.baseDir, "glob-merge-*")
	if err != nil {
		return err
	}
	// Defer removal of the temporary file in case of an error
	// before the atomic rename.
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()

	var chunkReader io.Reader

	finalHasher := sha512.New()

	for range totalChunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk := <-chunks:
			offset := chunk.index * c.width
			_, err = tempFile.WriteAt(chunk.Data, offset)
			if err != nil {
				return fmt.Errorf("failed to write chunk to temp file: %w", err)
			}

			chunkReader = bytes.NewReader(chunk.Data)
			if _, err = io.Copy(finalHasher, chunkReader); err != nil {
				return err
			}
		}
	}

	finalHash := finalHasher.Sum(nil)

	if !bytes.Equal(finalHash, totalHash.Bytes()) {
		return ErrChecksumNotMatched
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	return os.Rename(tempFile.Name(), p)
}
