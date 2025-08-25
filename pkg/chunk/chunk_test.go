package chunk

import (
	"crypto/rand"
	"crypto/sha512"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TODO: write a test which is not even chunk widths
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
			path, hash := prepareTempFile(t, tmpDir, tc.size)

			c := NewChunker(tc.width)
			m, err := c.Meta(path)
			require.NoError(t, err)

			require.Equal(t, hash, m.Hash)
			require.Len(t, m.Hashes, int(tc.chunks))

			for i, h := range m.Hashes {

				require.Len(t, h, 64)

				var hash [64]byte
				copy(hash[:], h)

				d, err := c.Data(path, hash, int64(i))
				require.NoError(t, err)

				require.Len(t, d, int(tc.width))
			}
		})
	}
}

func prepareTempFile(t *testing.T, dir string, size int64) (path string, hash []byte) {
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

	// Checksum the file we just wrote.
	ff, err := os.Open(f.Name())
	require.NoError(t, err)
	defer func() { _ = ff.Close() }()

	hasher := sha512.New()
	_, err = io.Copy(hasher, ff)
	require.NoError(t, err)

	return f.Name(), hasher.Sum(nil)
}

func hashFile(t *testing.T, path string) []byte {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	hasher := sha512.New()
	_, err = io.Copy(hasher, f)
	require.NoError(t, err)

	return hasher.Sum(nil)
}
