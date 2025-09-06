package chunk

import (
	"crypto/sha512"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	x, err := uuid.New().MarshalText()
	require.NoError(t, err)
	var h Hash = sha512.Sum512(x)

	b := h.Bytes()
	var hh Hash
	err = hh.FromBytes(b)
	require.NoError(t, err)

	require.Equal(t, hh.Bytes(), b)
	require.Equal(t, h.Bytes(), hh.Bytes())
}

func TestHashes(t *testing.T) {
	cases := []struct {
		name   string
		chunks int64
	}{
		{
			name:   "basic",
			chunks: 10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hashes := make(Hashes, tc.chunks)

			bb := make([][]byte, tc.chunks)

			for i := range tc.chunks {
				x, err := uuid.New().MarshalText()
				require.NoError(t, err)

				var h Hash = sha512.Sum512(x)
				hashes[i] = h

				b := h.Bytes()
				bb[i] = b
				// var hh Hash
				// err = hh.FromBytes(b)
				// require.NoError(t, err)

				// require.Equal(t, hh.Bytes(), b)
				// require.Equal(t, h.Bytes(), hh.Bytes())
			}

			var x Hashes
			err := x.FromBytes(bb)
			require.NoError(t, err)

			require.Equal(t, x.Bytes(), hashes.Bytes())
		})
	}
}
