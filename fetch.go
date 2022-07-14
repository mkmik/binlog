package main

import (
	"context"
	"encoding/binary"
	"io"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	v1 "mkm.pub/binlog/proto"
)

type FetchCmd struct {
	Source  string `required:"" help:"address of a Source gRPC server"`
	Origin  string `optional:""`
	TraceID string `required:""`
}

func (cmd *FetchCmd) Run(cli *Context) error {
	ctx := context.Background()
	client, err := grpc.DialContext(ctx, cmd.Source, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	reader := v1.NewLogReaderServiceClient(client)

	req := &v1.ReadRequest{
		Origin:  cmd.Origin,
		TraceId: cmd.TraceID,
	}
	res, err := reader.Read(ctx, req)
	if err != nil {
		return err
	}
	w := os.Stdout
	for {
		r, err := res.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		b, err := proto.Marshal(r.Entry)
		if err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}
