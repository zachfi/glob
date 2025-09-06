// Package app - module management for glob
package app

import (
	"context"
	"fmt"
	"os"

	kitlog "github.com/go-kit/log"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/zachfi/glob/modules/hasher"
	"github.com/zachfi/glob/modules/operator"
	"github.com/zachfi/glob/modules/syncer"
	"github.com/zachfi/glob/modules/watcher"
	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

const (
	Server string = "server"

	Operator string = "operator"
	Watcher  string = "watcher"
	Syncer   string = "syncer"
	Hasher   string = "hasher"

	All string = "all"
)

func (a *App) setupModuleManager() error {
	mm := modules.NewManager(kitlog.NewLogfmtLogger(os.Stderr))
	mm.RegisterModule(Server, a.initServer, modules.UserInvisibleModule)

	// TODO: a module which creates a local keypair for use on the server before it starts.

	mm.RegisterModule(Operator, a.initOperator)
	mm.RegisterModule(Watcher, a.initWatcher)
	mm.RegisterModule(Syncer, a.initSyncer)
	mm.RegisterModule(Hasher, a.initHasher)

	mm.RegisterModule(All, nil)

	deps := map[string][]string{
		// Server:       nil,

		Watcher:  {Server},
		Hasher:   {Server, Watcher},
		Operator: {Server, Hasher},
		Syncer:   {Server, Hasher},

		All: {
			Operator,
			Watcher,
		},
	}

	for mod, targets := range deps {
		if err := mm.AddDependency(mod, targets...); err != nil {
			return err
		}
	}

	a.ModuleManager = mm

	return nil
}

func (a *App) initWatcher() (services.Service, error) {
	w, err := watcher.New(a.cfg.Watcher, a.logger)
	if err != nil {
		return nil, err
	}

	a.watcher = w
	return w, nil
}

func (a *App) initHasher() (services.Service, error) {
	h, err := hasher.New(a.cfg.Hasher, a.logger, a.watcher.Receive())
	if err != nil {
		return nil, err
	}

	a.hasher = h
	return h, nil
}

func (a *App) initSyncer() (services.Service, error) {
	s, err := syncer.New(a.cfg.Syncer, a.logger, a.hasher.Receive())
	if err != nil {
		return nil, err
	}

	globv1proto.RegisterSyncServiceServer(a.Server.GRPC, s)

	a.syncer = s
	return s, nil
}

func (a *App) initOperator() (services.Service, error) {
	o, err := operator.New(a.cfg.Operator, a.logger, a.hasher.Receive())
	if err != nil {
		return nil, err
	}

	a.operator = o
	return o, nil
}

func (a *App) initServer() (services.Service, error) {
	a.cfg.Server.MetricsNamespace = metricsNamespace
	a.cfg.Server.ExcludeRequestInLog = false
	a.cfg.Server.RegisterInstrumentation = true
	a.cfg.Server.DisableRequestSuccessLog = false
	// a.cfg.Server.Log = a.logger

	if a.cfg.EnableGoRuntimeMetrics {
		// unregister default Go collector
		prometheus.Unregister(collectors.NewGoCollector())
		// register Go collector with all available runtime metrics
		prometheus.MustRegister(collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
		))
	}

	/* DisableSignalHandling(&t.cfg.Server) */

	server, err := server.New(a.cfg.Server)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create server")
	}

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)
		for m, s := range a.serviceMap {
			// Server should not wait for itself.
			if m != Server {
				svs = append(svs, s)
			}
		}
		return svs
	}

	a.Server = server

	serverDone := make(chan error, 1)

	runFn := func(ctx context.Context) error {
		go func() {
			defer close(serverDone)
			serverDone <- server.Run()
		}()

		select {
		case <-ctx.Done():
			return nil
		case err := <-serverDone:

			if errors.Is(err, context.Canceled) {
				return nil
			}

			if err != nil {
				return err
			}
			return fmt.Errorf("server stopped unexpectedly")
		}
	}

	stoppingFn := func(_ error) error {
		// wait until all modules are done, and then shutdown server.
		for _, s := range servicesToWaitFor() {
			_ = s.AwaitTerminated(context.Background())
		}

		// shutdown HTTP and gRPC servers (this also unblocks Run)
		server.Shutdown()

		// if not closed yet, wait until server stops.
		<-serverDone
		a.logger.Info("server stopped")
		return nil
	}

	return services.NewBasicService(nil, runFn, stoppingFn), nil
}
