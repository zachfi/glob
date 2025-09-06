package chunk

import globv1 "github.com/zachfi/glob/proto/glob/v1"

type ChunkMeta struct {
	hash              Hash // the hash of the chunk
	*globv1.ChunkMeta      // The chunk meta
}

func (m *ChunkMeta) FromProto(o *globv1.ChunkMeta) *ChunkMeta {
	var hash Hash
	hash.FromBytes(o.Hash)

	m = &ChunkMeta{
		hash:      hash,
		ChunkMeta: o,
	}

	return m
}
