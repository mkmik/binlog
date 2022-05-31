package reader

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/mkmik/tail"
	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
)

// ReadFileInto reads log entries from filename and writes to channel res, optionally "follwing" the
// file for new appended data.
func ReadFileInto(ctx context.Context, filename string, res chan *v1.GrpcLogEntry, follow bool) error {
	r, err := openFile(ctx, filename, follow)
	if err != nil {
		return err
	}
	return ReadInto(ctx, r, res)
}

func openFile(ctx context.Context, filename string, follow bool) (io.Reader, error) {
	if follow {
		t := tail.Follow(ctx, tail.LoggerFunc(log.Printf), filename,
			tail.Whence(io.SeekStart),
			tail.PollTimeout(time.Minute*10))
		return t, nil
	} else {
		return os.Open(filename)
	}
}

// ReadFile returns a channel of log entries and a channel containing the possible error.
// The entries channel is closed when an error is returned or when the input is fully consumed if follow is false.
// If follow is true the channel will only be closed when the context is canceled.
func ReadFile(ctx context.Context, filename string, follow bool) (chan *v1.GrpcLogEntry, chan error) {
	res := make(chan *v1.GrpcLogEntry)
	errCh := make(chan error, 1)

	go func() {
		errCh <- ReadFileInto(ctx, filename, res, follow)
		close(res)
		close(errCh)
	}()

	return res, errCh
}
