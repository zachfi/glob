package hasher

import (
	"os"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

type TableTarget interface {
	ChunkMeta(path string) (*globv1proto.ChunkMeta, error)
}

var _ TableTarget = (*targetWrapper)(nil)

type targetWrapper struct {
	t *globv1proto.Target
}

func (tw *targetWrapper) ChunkMeta(path string) (*globv1proto.ChunkMeta, error) {
	for _, o := range tw.t.Object {
		if o.ObjectStatus.Path == path {
			return o, nil
		}
	}

	return nil, os.ErrNotExist
}
