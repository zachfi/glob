// Package chunk is responsible for handling the chunking of a particular file into fixed size units.
package chunk

import (
	"bufio"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"os"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

const DefaultChunkSize = 1024 * 1024 // 1MB

// Chunker should be able to return a chunk meta of an object, which includes the indivulal chunk hashes, the overall hash, and the total size of the object.  Chunker should also be able to read an individual chunk by its hash.
type Chunker interface {
	Meta(path string) (*globv1proto.ChunkMeta, error)
	// TODO: are we using the hash to error if it doesn't match the data read?
	Data(path string, hash [64]byte, index int64) ([]byte, error)

	Merge(path string, hash [64]byte, data io.Reader) error
}

var _ Chunker = (*chunker)(nil)

type chunker struct {
	width int64
	// TODO: implement a temporary directory for merge to write into.  Atomically merge the file into place.
}

func NewChunker(width int64) Chunker {
	return &chunker{
		width: width,
	}
}

func (c *chunker) Meta(path string) (*globv1proto.ChunkMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)

	hashes, hash, size, err := c.chunkIt(reader)
	if err != nil {
		return nil, err
	}

	return &globv1proto.ChunkMeta{
		TotalBytes: size,
		Hashes:     hashes,
		Hash:       hash,
		ByteWidth:  DefaultChunkSize,
	}, nil
}

// chunkIt returns a slice of []byte representing list of hahes, the sum total hash
func (c *chunker) chunkIt(reader *bufio.Reader) (hashes [][]byte, hash []byte, size int64, err error) {
	var (
		chunk       = make([]byte, c.width)
		hasher      = sha512.New()
		totalHasher = sha512.New()
		bytesRead   int
	)

	for {
		bytesRead, err = io.ReadFull(reader, chunk)

		// We always process whatever we read, even if it's less than a full chunk.
		hasher.Write(chunk[:bytesRead])
		totalHasher.Write(chunk[:bytesRead])
		size += int64(bytesRead)

		if bytesRead > 0 {
			hashes = append(hashes, hasher.Sum(nil))
			hasher.Reset()
		}

		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, nil, 0, err
		}
	}

	return hashes, totalHasher.Sum(nil), size, nil
}

func (c *chunker) Data(path string, hash [64]byte, index int64) ([]byte, error) {
	f, err := os.Open(path)
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
	dataHash := sha512.Sum512(data[:i])
	if hash != dataHash {
		return nil, ErrChecksumNotMatched
	}

	return data, nil
}

func (c *chunker) Merge(path string, hash [64]byte, reader io.Reader) error {
	return nil
}
