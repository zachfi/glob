package syncer

import (
	"context"
	"log/slog"

	"github.com/grafana/dskit/services"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

var module = "syncer"

var tracer = otel.Tracer(module, trace.WithInstrumentationAttributes(attribute.String("module", module)))

type Syncer struct {
	services.Service

	cfg    *Config
	logger *slog.Logger

	// incoming events from the hasher
	ch chan *globv1proto.HashNotification
}

func New(cfg Config, logger *slog.Logger, ch chan *globv1proto.HashNotification) (*Syncer, error) {
	w := &Syncer{
		cfg:    &cfg,
		logger: logger.With("module", module),
		ch:     ch,
	}

	w.Service = services.NewBasicService(w.starting, w.running, w.stopping)
	return w, nil
}

func (s *Syncer) starting(_ context.Context) error {
	return nil
}

func (s *Syncer) running(ctx context.Context) error {
	return nil
}

func (s *Syncer) stopping(_ error) error {
	return nil
}

func (s *Syncer) GetTargetStatus(ctx context.Context, req *globv1proto.GetTargetStatusRequest) (*globv1proto.GetTargetStatusResponse, error) {
	ctx, span := tracer.Start(ctx, "GetTargetStatus")
	defer span.End()

	return nil, nil
}

func (s *Syncer) FetchChunks(ctx context.Context, req *globv1proto.FetchChunksRequest) (*globv1proto.FetchChunksResponse, error) {
	ctx, span := tracer.Start(ctx, "FetchChunks")
	defer span.End()

	return nil, nil
}

func (s *Syncer) Subscribe(req *globv1proto.SubscribeRequest, stream globv1proto.SyncService_SubscribeServer) error {
	return nil
}
