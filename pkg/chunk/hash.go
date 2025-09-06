package chunk

import (
	"fmt"
	"io"
)

type Hashes []Hash

func (hh Hashes) Bytes() (data [][]byte) {
	data = make([][]byte, len(hh))

	for i, d := range hh {
		data[i] = d.Bytes()
	}

	return data
}

func (hh *Hashes) FromBytes(bb [][]byte) error {
	d := make([]Hash, len(bb))
	var err error

	for i, b := range bb {
		var v Hash
		err = v.FromBytes(b)
		if err != nil {
			return err
		}
		d[i] = v
	}

	*hh = d

	return nil
}

type Hash [64]byte

// Bytes returns the Hash as a byte slice.
// This is done efficiently by converting the array pointer to a slice.
func (h *Hash) Bytes() []byte {
	// A slice is a descriptor for a contiguous sequence of memory.
	// We create a slice that points to the underlying array data.
	return h[:]
}

func (h *Hash) FromBytes(b []byte) error {
	if len(b) != 64 {
		return fmt.Errorf("incorrect byte slice length: got %d, want 64", len(b))
	}

	// We can convert a pointer to a 64-byte array to a pointer to a 64-byte array.
	// This is a zero-copy operation.
	*h = *(*[64]byte)(b)

	return nil
}

var _ io.Writer = (*Hash)(nil)

func (h *Hash) Write(b []byte) (int, error) {
	return 0, nil
}
