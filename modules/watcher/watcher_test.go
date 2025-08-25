package watcher

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	globv1proto "github.com/zachfi/glob/proto/glob/v1"
)

func TestWatcher(t *testing.T) {
	var (
		loggerOpts  = &slog.HandlerOptions{Level: slog.LevelDebug}
		handler     = slog.NewTextHandler(os.Stdout, loggerOpts)
		logger      = slog.New(handler)
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	)
	defer cancel()

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})
	cfg.NotificationDelay = 100 * time.Millisecond

	w, err := New(cfg, logger)
	require.NoError(t, err)

	err = w.starting(ctx)
	require.NoError(t, err)

	go func() {
		err := w.running(ctx)
		require.NoError(t, err)
	}()

	tmpDir := t.TempDir()

	id := []byte("asd")
	err = w.Add(ctx, id, tmpDir)
	require.NoError(t, err)

	received := make(map[globv1proto.ObjectOp]int32)

	go func() {
		defer func() {
			for _, c := range received {
				require.Equal(t, int32(10), c)
			}
		}()

		ch := w.Receive()

		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				received[e.Op]++
			}
		}
	}()

	err = w.Add(ctx, id, tmpDir)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	go func() {
		for range 10 {
			f, err := os.CreateTemp(tmpDir, "test")
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)

			data := []byte("data!")
			_, err = f.Write(data)
			require.NoError(t, err)
			err = f.Close()
			require.NoError(t, err)

			b, err := os.ReadFile(f.Name())
			require.NoError(t, err)

			require.Equal(t, data, b)
		}
	}()

	time.Sleep(3 * time.Second)
}
