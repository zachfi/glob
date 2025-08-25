package watcher

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/grafana/dskit/services"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

var module = "watcher"

var tracer = otel.Tracer(module, trace.WithInstrumentationAttributes(attribute.String("module", module)))

type targetFlushMap map[string]object

type object map[string]*flush

type flush struct {
	notification *globv1proto.ObjectNotification
	lastWrite    time.Time
}

type Watcher struct {
	services.Service

	cfg    *Config
	logger *slog.Logger

	events chan *globv1proto.ObjectNotification

	flushTrackerMtx sync.Mutex
	flushTracker    map[string]map[string]*flush

	targetFlushMap targetFlushMap
}

func New(cfg Config, logger *slog.Logger) (*Watcher, error) {
	w := &Watcher{
		cfg:            &cfg,
		logger:         logger.With("module", module),
		events:         make(chan *globv1proto.ObjectNotification, 100),
		targetFlushMap: make(targetFlushMap),
	}

	w.Service = services.NewBasicService(w.starting, w.running, w.stopping)

	return w, nil
}

func (w *Watcher) starting(ctx context.Context) error {
	// Top level key is the string ID of the target.  Next string key is the path.
	w.flushTracker = make(map[string]map[string]*flush)

	return nil
}

func (w *Watcher) running(ctx context.Context) error {
	flushCheckTicker := time.NewTicker(w.cfg.NotificationDelay)
	defer flushCheckTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-flushCheckTicker.C:
				w.flushTrackerMtx.Lock()

				for id := range w.flushTracker {
					for p := range w.flushTracker[id] {
						if time.Since(w.flushTracker[id][p].lastWrite) > w.cfg.NotificationDelay {
							w.events <- w.flushTracker[id][p].notification
							delete(w.flushTracker[id], p)
						}
					}
				}

				w.flushTrackerMtx.Unlock()
			}
		}
	}()

	<-ctx.Done()
	return nil
}

func (w *Watcher) stopping(_ error) error {
	return nil
}

// TODO: use this function to add the share paths.  Expose a public method to
// avoid needing to know the shares in the watcher.Config.
func (w *Watcher) Add(ctx context.Context, targetID []byte, path string) error {
	if len(targetID) == 0 {
		return fmt.Errorf("unable to add path %q for empty targetID", path)
	}

	w.logger.Info("watching directory", "path", path)

	// TODO: create a cancle func for the received context, store the cancel for later

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	id := hex.EncodeToString(targetID)

	go func() {
		defer func() {
			err := watcher.Close()
			if err != nil {
				w.logger.Error("failed to close watcher", "err", err)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				w.logger.Info("context", "ctx.Done()", ctx.Done())

				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				w.logger.Debug("received", "event", fmt.Sprintf("%+v", event), "ok", ok)

				watcher.WatchList()

				// TODO:implement the channel receiver in the hasher.

				if event.Has(fsnotify.Write) {
					go w.trackWrite(targetID, id, event)
					continue
				}

				// TODO: capture the other events

				// if event.Has(fsnotify.Remove) {
				// }
				//
				// if event.Has(fsnotify.Rename) {
				// }
				//
				// if event.Has(fsnotify.Create) {
				// }
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}

				w.logger.Error("watcher error", "err", err)
			}
		}
	}()

	return watcher.Add(path)
}

func (w *Watcher) Remove(path string) error {
	// TODO:
	// return watcher.Remove(path)
	return nil
}

// Receive returns a channel that receives file system events.
//
// TODO: consider a fan-in/fan-out model if multiple consumers are needed.
func (w *Watcher) Receive() chan *globv1proto.ObjectNotification {
	return w.events
}

func (w *Watcher) trackWrite(id []byte, hexID string, event fsnotify.Event) {
	w.flushTrackerMtx.Lock()
	defer w.flushTrackerMtx.Unlock()

	if _, ok := w.flushTracker[hexID]; !ok {
		w.flushTracker[hexID] = make(map[string]*flush)
	}

	if _, ok := w.flushTracker[hexID][event.Name]; ok {
		w.flushTracker[hexID][event.Name].lastWrite = time.Now()
		return
	}

	var op globv1proto.ObjectOp
	if event.Has(fsnotify.Write) {
		op = globv1proto.ObjectOp_OBJECT_OP_CHANGE
	}

	if event.Has(fsnotify.Remove) {
		// TODO: a remove should clean up the map
		op = globv1proto.ObjectOp_OBJECT_OP_REMOVE
	}

	// if event.Has(fsnotify.Rename) {
	// TODO: a rename event should move the map entry.
	// }

	if event.Has(fsnotify.Create) {
		op = globv1proto.ObjectOp_OBJECT_OP_ADD
	}

	f := &flush{
		notification: &globv1proto.ObjectNotification{
			TargetId: id,
			Path:     event.Name,
			Op:       op,
		},

		lastWrite: time.Now(),
	}

	w.flushTracker[hexID][event.Name] = f
}
