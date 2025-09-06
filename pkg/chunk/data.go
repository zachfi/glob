package chunk

import globv1 "github.com/zachfi/glob/proto/glob/v1"

// chunkData is the message passed through the channel.
type ChunkData struct {
	index             int64 // the index of the chunk
	total             int64 // the total number of chunks
	*globv1.ChunkData       // The data of chunk and its hash
}
