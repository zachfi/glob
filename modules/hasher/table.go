package hasher

import (
	"fmt"
	"sync"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

type Table interface {
	// addTarget(target *globv1proto.Target)
	// removeTarget(targetID string)
	// getTarget(targetID string) (TableTarget, error)
	// hasTarget(targetID string) bool

	hashObject(targetID string, path string, object *globv1proto.ChunkMeta) (globv1proto.HashNotification, error)
	deleteObject(targetID string, path string) (globv1proto.HashNotification, error)
}

var _ Table = (*hashTable)(nil)

// hashTable is a simple in-memory index of file hashes by target ID and path.
// The nested fields are proto entries, which makes serialization on disk or in
// object storage later easier and more performant.
type hashTable struct {
	targets map[string]*globv1proto.Target
	mtx     sync.Mutex
}

func newHashTable() *hashTable {
	return &hashTable{
		targets: make(map[string]*globv1proto.Target, 0),
	}
}

func (ht *hashTable) addTarget(target *globv1proto.Target) {
	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	ht.targets[string(target.Id)] = target
}

func (ht *hashTable) removeTarget(targetID string) {
	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	delete(ht.targets, targetID)
}

var ErrTargetNotFound = fmt.Errorf("target not found")

func (ht *hashTable) getTarget(targetID string) (TableTarget, error) {
	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	t, ok := ht.targets[targetID]
	if !ok {
		return nil, ErrTargetNotFound
	}

	return &targetWrapper{t: t}, nil
}

func (ht *hashTable) hasTarget(targetID string) bool {
	ht.mtx.Lock()
	defer ht.mtx.Unlock()

	_, ok := ht.targets[targetID]
	return ok
}
