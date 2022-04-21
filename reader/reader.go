package reader

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/proto"
)

// ReadInto reads log entries from r and writes to channel res.
func ReadInto(r io.Reader, res chan *v1.GrpcLogEntry) error {
	r = bufio.NewReader(r)

	for {
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error reading count: %w", err)
		}

		size := binary.BigEndian.Uint32(hdr)

		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				log.Printf("last entry truncated, ignoring")
				return nil
			}
			return fmt.Errorf("error reading body: %#v %w", err, err)
		}
		var entry v1.GrpcLogEntry
		if err := proto.Unmarshal(body, &entry); err != nil {
			return err
		}
		res <- &entry
	}
	return nil
}

// Read returns a channel of log entries and a channel containing the possible error.
// The entries channel is closed when an error is returned or when the input is fully consumed.
func Read(r io.Reader) (chan *v1.GrpcLogEntry, chan error) {
	res := make(chan *v1.GrpcLogEntry)
	errCh := make(chan error, 1)

	go func() {
		errCh <- ReadInto(r, res)
		close(res)
		close(errCh)
	}()

	return res, errCh
}
