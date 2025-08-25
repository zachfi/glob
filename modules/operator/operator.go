package operator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/services"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	"github.com/go-kit/log"
	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

var module = "operator"

var tracer = otel.Tracer(module, trace.WithInstrumentationAttributes(attribute.String("module", module)))

// PeerUpdate represents an update from a peer, including the peer ID and the
// list of updates.
type peerUpdate struct {
	Addr    string
	Updates []*grpcutil.Update
}

// Operator is responsible for maintaining all of the grpc clients and
// subscribing to the sync servers for each target.
type Operator struct {
	services.Service

	cfg    *Config
	logger *slog.Logger

	// Map of peer ID to grpc client connection.
	peerConns    map[string]*grpc.ClientConn
	peerConnsMtx sync.Mutex

	peerStreams    map[string][]globv1proto.SyncService_SubscribeClient
	peerStreamsMtx sync.Mutex

	updates chan *globv1proto.SubscribeResponse

	// incoming events from the hasher
	ch chan *globv1proto.HashNotification
}

func New(cfg Config, logger *slog.Logger, ch chan *globv1proto.HashNotification) (*Operator, error) {
	c := &Operator{
		cfg:         &cfg,
		logger:      logger.With("module", module),
		peerConns:   make(map[string]*grpc.ClientConn),
		peerStreams: make(map[string][]globv1proto.SyncService_SubscribeClient),
		updates:     make(chan *globv1proto.SubscribeResponse, 100),
		ch:          ch,
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c, nil
}

func (o *Operator) starting(_ context.Context) error {
	return nil
}

func (o *Operator) running(ctx context.Context) error {
	// TODO: start a go routine for each target which maintains a grpc client connection to each peer.

	err := o.syncPeers(ctx)
	if err != nil {
		return err
	}

	// bg := boundedwaitgroup.New(o.cfg.SyncConcurrency)

	// TODO: For each peer connection, start a go routine which calls Subscribe and processes incoming items.
	for {
		select {
		case <-ctx.Done():
			// 		bg.Wait()
			return nil
		case <-o.ch:
			//
			// 		bg.Add(1)
			// 		metricActiveReceiverRoutines.Inc()
			// 		go func() {
			// 			defer bg.Done()
			// 			defer metricActiveReceiverRoutines.Dec()
			// 			var err error
			// 			_, span := r.tracer.Start(i.ctx, "Router.receiver")
			// 			defer tracing.ErrHandler(span, err, "send failed", r.logger)
			// 			err = r.send(i.ctx, i.Path, i.Payload)
			// 			if err != nil {
			// 				r.logger.Error("failed to send item", "path", i.Path, "err", err)
			// 			}
			// 		}()
		}
	}
}

func (o *Operator) stopping(_ error) error {
	return nil
}

func (o *Operator) syncPeers(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "Operator.runTargets")
	defer span.End()

	for _, peer := range o.cfg.SyncPeers {
		o.logger.Info("starting peer", "id", peer.Addr)

		// Use dskit resolver to keep the DNS address updated with the latest
		// targets.  A target DNS could change, for example in a kubernetes
		// headless service.  When the list of addresses changes, we need to update
		// our internal state of peer cliet grpc connections.

		// Since each SyncTarget could have multiple addresses, we need to handle this gracefully as new addresses are added or removed.

		resolver, err := grpcutil.NewDNSResolver(log.NewLogfmtLogger(os.Stderr))
		if err != nil {
			return fmt.Errorf("failed to create DNS resolver: %w", err)
		}

		watcher, err := resolver.Resolve(peer.Addr, "")
		if err != nil {
			return fmt.Errorf("failed to resolve address %s: %w", peer.Addr, err)
		}

		// TODO: consider a struct here which wraps the updates.  We need to know
		// the peer Address here, since we could resolve to multiple IP addresses.
		// We need this relationship later when we create the grpc client
		// connections.
		//
		// TODO: Later when we have only the IP address, which peer does it belong to, so
		// that we know which targets to subscribe to?

		dnsUpdatesChan := make(chan peerUpdate)

		// Start a go routine to watch for updates from the DNS watcher.  Updates are written to the received channel.
		go func(ctx context.Context, addr string, ch chan peerUpdate) {
			defer close(ch)
			defer watcher.Close()
			// Loop forever, waiting for updates.
			for {
				select {
				case <-ctx.Done():
					return
				default:
					u, err := watcher.Next()
					if err != nil {
						o.logger.Error("failed to get next update", "addr", peer.Addr, "err", err)
						continue
					}
					ch <- peerUpdate{Addr: addr, Updates: u}
				}
			}
		}(ctx, peer.Addr, dnsUpdatesChan)

		// Start a go routine to process updates from the received channel.  New
		// entries are added to the peerConns map, and removed entries are removed
		// from the map.  For each target in the peer config, a new stream is
		// created to sync data.
		go func(ctx context.Context, p SyncPeer) {
			for {
				select {
				case <-ctx.Done():
					return
				case updates := <-dnsUpdatesChan:
					err := o.handlePeerUpdates(ctx, updates)
					if err != nil {
						o.logger.Error("failed to handle updates", "addr", p.Addr, "err", err)
					}
				}
			}
		}(ctx, peer)

		// Start a go routine to process incoming updates from all peers.
		go func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				case update := <-o.updates:
					// Process the update.
					o.logger.Info("received update", "update", update)
					o.handleSubscribeResponse(ctx, update)
				}
			}
		}(ctx)

	}

	return nil
}

func (o *Operator) handlePeerUpdates(ctx context.Context, update peerUpdate) error {
	o.peerConnsMtx.Lock()
	defer o.peerConnsMtx.Unlock()

	for _, u := range update.Updates {
		switch u.Op {
		case grpcutil.Add:
			o.logger.Info("adding peer connection", "addr", u.Addr)
			if _, exists := o.peerConns[u.Addr]; exists {
				o.logger.Info("peer connection already exists", "addr", u.Addr)
				continue
			}
			// TODO: Update the configuration of the grpc client.  We will eventually want mutulal TLS here.
			conn, err := grpc.DialContext(ctx, u.Addr, grpc.WithInsecure(), grpc.WithBlock())
			if err != nil {
				o.logger.Error("failed to dial peer", "addr", u.Addr, "err", err)
				continue
			}

			o.peerConns[u.Addr] = conn

			// NOTE: we use the original peer address to look up the targets, since
			// this is what is stored in the config.
			targets := o.targetsForPeer(update.Addr)
			o.initPeerStreams(ctx, u.Addr, conn, targets)

			o.logger.Info("peer connection added", "addr", u.Addr)
		case grpcutil.Delete:

			// deinit streams for this peer
			peer := SyncPeer{Addr: u.Addr}
			o.deInitPeerStreams(ctx, peer)

			// close and remove the grpc client connection
			o.logger.Info("removing peer connection", "addr", u.Addr)
			if conn, exists := o.peerConns[u.Addr]; exists {
				err := conn.Close()
				if err != nil {
					o.logger.Error("failed to close peer connection", "addr", u.Addr, "err", err)
				}
				delete(o.peerConns, u.Addr)
				o.logger.Info("peer connection removed", "addr", u.Addr)
			} else {
				o.logger.Info("peer connection does not exist", "addr", u.Addr)
			}
		}
	}

	return nil
}

func (o *Operator) initPeerStreams(ctx context.Context, addr string, conn *grpc.ClientConn, targets []SyncTarget) {
	o.peerStreamsMtx.Lock()
	defer o.peerStreamsMtx.Unlock()

	streams, exists := o.peerStreams[addr]
	if !exists {
		streams = make([]globv1proto.SyncService_SubscribeClient, len(o.cfg.SyncTargets))
	}

	for _, target := range targets {
		id, err := target.ID.MarshalText()
		if err != nil {
			o.logger.Error("failed to marshal target ID", "peer", addr, "target_id", target.ID, "err", err)
			continue
		}

		stream, err := globv1proto.NewSyncServiceClient(conn).Subscribe(ctx, &globv1proto.SubscribeRequest{
			TargetId: id,
		})
		if err != nil {
			o.logger.Error("failed to subscribe to target", "peer", addr, "target_id", target.ID, "err", err)
			continue
		}

		streams = append(streams, stream)

		// Start a go routine to read from the stream and write to the channel.
		go func(ctx context.Context, s globv1proto.SyncService_SubscribeClient, peerAddr string, targetID uuid.UUID) {
			for {
				resp, err := s.Recv()
				if err != nil {
					o.logger.Error("failed to receive from stream", "peer", peerAddr, "target_id", targetID, "err", err)
					return
				}
				o.updates <- resp
			}
		}(ctx, stream, addr, target.ID)
	}

	o.peerStreams[addr] = streams
}

// deInitPeerStreams closes all streams for a given peer.
func (o *Operator) deInitPeerStreams(ctx context.Context, peer SyncPeer) {
	o.peerStreamsMtx.Lock()
	defer o.peerStreamsMtx.Unlock()

	streams, exists := o.peerStreams[peer.Addr]
	if !exists {
		return
	}

	for _, stream := range streams {
		if stream != nil {
			// TODO: CloseSend or just close?  We'll be calling here when a peer
			// address is removed, which means it may not be at the old address.  I
			// believe the CloseSend blocks on the Send, which would be bad if the
			// address is no longer valid.

			err := stream.CloseSend()
			if err != nil {
				o.logger.Error("failed to close stream", "peer", peer.Addr, "err", err)
			}
		}
	}

	delete(o.peerStreams, peer.Addr)
}

// targetsForPeer returns the list of targets for a given peer address.
//
// NOTE: the received target must be the original name specified in the config,
// not the resolved address.
func (o *Operator) targetsForPeer(peerAddr string) []SyncTarget {
	var targets []SyncTarget
	for _, peer := range o.cfg.SyncPeers {
		if peer.Addr == peerAddr {
			for _, targetID := range peer.Targets {
				target := o.getTargetByID(targetID)
				if target == nil {
					continue
				}

				targets = append(targets, *target)
			}
		}
	}
	return targets
}

func (o *Operator) getTargetByID(id uuid.UUID) *SyncTarget {
	for _, target := range o.cfg.SyncTargets {
		if target.ID == id {
			return &target
		}
	}
	return nil
}

func (o *Operator) handleSubscribeResponse(ctx context.Context, resp *globv1proto.SubscribeResponse) {
	// TODO: implement the handling of subscriber updates.
	//
	// If an Object has been created, we need to pull it down. First we should
	// get the file chunk hashes from the update.  If it doesn't exist, extend
	// the proto and ensure it comes across.
	//
	// Next we should ask all of our peers if they have these chunks of the file.
	// If they do, then we should divide the chunks we need among the peers which
	// have them and start pulling them down.  Store the chunks in a temporary
	// location so that when they are all downloaded, we can assemble them into
	// the final file to be atomically moved into place.
	//
	// If the object was changed, we should check the file hash against our own
	// local copy, and if it is different, use the latest timestamp to determine
	// if we should pull it down.  If we should pull it down, then we should get
	// the hash chunks like the create object and then atomically move the new
	// file into place.  Once in place, the hash should match the expected hash.
	//
	// If the object was removed, we should delete it locally.
	//
	// All hash checking operations should be present from the hasher, and we
	// should not need to touch the file contents directly.
}
