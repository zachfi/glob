package hasher

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

func TestHasher(t *testing.T) {
	var (
		loggerOpts  = &slog.HandlerOptions{Level: slog.LevelDebug}
		handler     = slog.NewTextHandler(os.Stdout, loggerOpts)
		logger      = slog.New(handler)
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	)
	defer cancel()

	cfg := Config{}
	cfg.RegisterFlagsAndApplyDefaults("", &flag.FlagSet{})

	ch := make(chan *globv1proto.ObjectNotification)

	h, err := New(cfg, logger, ch)
	require.NoError(t, err)
	require.NotNil(t, h)

	err = h.starting(ctx)
	require.NoError(t, err)

	go func() {
		err := h.running(ctx)
		require.NoError(t, err)
	}()

	tmpDir := t.TempDir()

	go func() {
		for range 10 {
			f, err := os.CreateTemp(tmpDir, "test")
			require.NoError(t, err)

			data := []byte("data")

			_, err = f.Write(data)
			require.NoError(t, err)
			err = f.Close()
			require.NoError(t, err)

			ch <- &globv1proto.ObjectNotification{
				TargetId: []byte("asd"),
				Path:     f.Name(),
				Op:       globv1proto.ObjectOp_OBJECT_OP_ADD,
			}

			// b, err := os.ReadFile(f.Name())
			// require.NoError(t, err)
			//
			// require.Equal(t, data, b)
		}
	}()

	time.Sleep(3 * time.Second)
}
