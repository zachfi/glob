package chunk

import (
	"context"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	globv1 "github.com/zachfi/glob/proto/glob/v1"
)

func TestChunker(t *testing.T) {
	cases := []struct {
		name   string
		width  int64
		chunks int64
		size   int64
	}{
		{
			name:   "basic defaults",
			width:  DefaultChunkSize,
			chunks: 10,
			size:   1024 * 1024 * 10,
		},
		{
			name:   "basic with extra bytes",
			width:  DefaultChunkSize,
			chunks: 11,
			size:   (1024 * 1024 * 10) + 32,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			path, hash := prepareTempFile(t, tmpDir, tc.size)

			c, err := NewChunker(tc.width, tmpDir)
			require.NoError(t, err)

			tmpFilePath := filepath.Base(path)

			m, err := c.Meta(tmpFilePath)
			require.NoError(t, err)

			require.Equal(t, hash.Bytes(), m.Hash)
			require.Len(t, m.Hashes, int(tc.chunks))

			data := make([][]byte, len(m.Hashes))

			for i, h := range m.Hashes {
				var hash Hash
				err = hash.FromBytes(h)
				require.NoError(t, err)

				d, err := c.Data(tmpFilePath, hash, int64(i))
				require.NoError(t, err)
				data[i] = d
			}

			ch := make(chan ChunkData)
			defer close(ch)

			bg := sync.WaitGroup{}

			bg.Add(1)
			go func() {
				defer bg.Done()
				err := c.Merge(ctx, "file.bin", m.hash, int64(len(m.Hashes)), ch)
				require.NoError(t, err)
			}()

			bg.Add(1)
			go func() {
				defer bg.Done()
				for i := range data {
					select {
					case <-ctx.Done():
						return
					case ch <- ChunkData{
						index: int64(i),
						ChunkData: &globv1.ChunkData{
							Hash: m.Hashes[i],
							Data: data[i],
						},
					}:
					}
				}
			}()

			bg.Wait()
		})
	}
}

func prepareTempFile(t *testing.T, dir string, size int64) (path string, hash Hash) {
	f, err := os.CreateTemp(dir, "chunk")
	require.NoError(t, err)

	data := make([]byte, size)
	randomBytesWritten, err := rand.Read(data)
	require.NoError(t, err)
	require.Equal(t, int64(randomBytesWritten), size)

	fileBytesWritten, err := f.Write(data)
	require.NoError(t, err)
	require.Equal(t, int64(fileBytesWritten), size)

	err = f.Close()
	require.NoError(t, err)

	return f.Name(), hashFile(t, f.Name())
}

func hashFile(t *testing.T, path string) Hash {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() {
		funcErr := f.Close()
		require.NoError(t, funcErr)
	}()

	hasher := sha512.New()
	_, err = io.Copy(hasher, f)
	require.NoError(t, err)

	var dataHash Hash
	err = dataHash.FromBytes(hasher.Sum(nil))
	require.NoError(t, err)

	fmt.Printf("hashFile.dataHash: %+v\n", dataHash)

	return dataHash
}
