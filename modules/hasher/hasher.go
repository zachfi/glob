package hasher

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/grafana/dskit/services"
	"github.com/zachfi/zkit/pkg/boundedwaitgroup"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/zachfi/glob/pkg/chunk"
	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

var module = "hasher"

var tracer = otel.Tracer(module, trace.WithInstrumentationAttributes(attribute.String("module", module)))

type Hasher struct {
	services.Service

	cfg    *Config
	logger *slog.Logger

	// incoming events from the watcher
	events chan *globv1proto.ObjectNotification

	// outgoing events to the operator and syncer
	hashEvents    []chan *globv1proto.HashNotification
	hashEventsMtx sync.Mutex

	hashTable Table
	chunker   chunk.Chunker
}

func New(cfg Config, logger *slog.Logger, events chan *globv1proto.ObjectNotification) (*Hasher, error) {
	w := &Hasher{
		cfg:        &cfg,
		logger:     logger.With("module", module),
		events:     events,
		hashEvents: make([]chan *globv1proto.HashNotification, 0),
		hashTable:  newHashTable(),
	}

	w.chunker = chunk.NewChunker()

	w.Service = services.NewBasicService(w.starting, w.running, w.stopping)
	return w, nil
}

func (h *Hasher) starting(_ context.Context) error {
	return nil
}

func (h *Hasher) running(ctx context.Context) error {
	bg := boundedwaitgroup.New(h.cfg.ConcurrencyLimit)

	for {
		select {
		case <-ctx.Done():
			h.logger.Debug("context", "ctx.Err()", ctx.Err())

			bg.Wait()
			return nil
		case e := <-h.events:
			if len(e.TargetId) == 0 {
				h.logger.Warn("received event with no target ID: %+v", "target_id", e.TargetId)
				continue
			}

			bg.Add(1)
			go func() {
				defer bg.Done()

				// TODO: actually hash the file.  Generate the HashNotification.  Write
				// the notification to all subscribers which have called Receive().

				var (
					meta *globv1proto.ChunkMeta
					err  error
				)

				// TODO: if the event is an add, or change, get the chunk meta and store it in the table.
				// If the event is a remove, remove it from the table.
				// If the event is a rename, update the path in the table.
				// For all events, notify the subscribers of the change.

				// TODO: add tracing

				// TODO: what does the operator do with the hash notification?  Since
				// it is the client, and the peer handler, perhaps nothing, and in
				// reality the syncer is the only one who cares about it?

				// TODO: operator: When the operator receives a subcsribe notification
				// from the peers, it should determine if the update is the most
				// recent.  If the update is the most recent, we need to figure out
				// which hashes we need to get from the peers.

				// TODO: what does the syncer do with the hash notification?  It needs
				// to let all subscribers know of the change.

				// TODO: if the local event has hashes which area already present in
				// the table, don't notify.  Is this enough to prevent loops?

				// TODO: Are there two tables?  One which is local and one which is global?
				// Then we seek to make them consistent?

				switch e.Op {
				case globv1proto.ObjectOp_OBJECT_OP_ADD:

					meta, err = chunk.GetChunk(e.Path)
					if err != nil {
						h.logger.Error("failed to chunk file", "path", e.Path, "err", err, "target_id", string(e.TargetId))
						return
					}

					// TODO: update the index with the new file.
					h.hashTable.addTarget(e.TargetId)

				case globv1proto.ObjectOp_OBJECT_OP_CHANGE:

					meta, err = chunk.GetChunk(e.Path)
					if err != nil {
						h.logger.Error("failed to chunk file", "path", e.Path, "err", err, "target_id", string(e.TargetId))
						return
					}

				case globv1proto.ObjectOp_OBJECT_OP_REMOVE:
					// TODO: who has the responsibility to delete the file from disk?
					err := os.Remove(e.Path)
					if err != nil {
						h.logger.Error("failed to remove file", "path", e.Path, "err", err)
					}
					// Remove the file from the index.

					h.hashTable.removeTarget(string(e.TargetId))

				case globv1proto.ObjectOp_OBJECT_OP_RENAME:
					// TODO:

				}

				hn := &globv1proto.HashNotification{
					TargetId:  e.TargetId,
					Path:      e.Path,
					ChunkMeta: meta,
				}

				h.hashEventsMtx.Lock()
				defer h.hashEventsMtx.Unlock()

				// Fan-in.  Receive from the h.Events, and write the notification to all subscribers.
				for _, ch := range h.hashEvents {
					ch <- hn
				}
			}()
		}
	}
}

func (h *Hasher) stopping(_ error) error {
	return nil
}

func (h *Hasher) rehash(file string) error {
	return nil
}

func (h *Hasher) Receive() chan *globv1proto.HashNotification {
	ch := make(chan *globv1proto.HashNotification, 1)
	h.hashEventsMtx.Lock()
	defer h.hashEventsMtx.Unlock()

	h.hashEvents = append(h.hashEvents, ch)

	return ch
}
