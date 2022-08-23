package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	v1 "mkm.pub/binlog/proto"
	"mkm.pub/binlog/reader"
)

type SendCmd struct {
	CmdCommon

	Target string `required:"" help:"address of a target gRPC server"`
	Origin string `required:""`
	Prefix string `required:"" help:"call-id prefix"`

	Headers []string `short:"H" long:"header" help:"custom http header(s)"`
}

func (cmd *SendCmd) Run(cli *Context) error {
	ctx := context.Background()
	client, err := grpc.DialContext(ctx, cmd.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	sink := v1.NewLogSinkServiceClient(client)

	entries, errCh := reader.ReadFile(ctx, cmd.LogInputFile, cli.Follow)

	for _, keyValue := range cmd.Headers {
		k, v, found := strings.Cut(keyValue, ":")
		if !found {
			return fmt.Errorf(`expected: "HeaderKey: header value", got: %q`, keyValue)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, k, strings.TrimLeft(v, " "))
	}
	for e := range entries {
		req := &v1.WriteRequest{
			Origin: cmd.Origin,
			CallId: fmt.Sprintf("%s-%d", cmd.Prefix, e.CallId),
			Entry:  e,
		}
		_, err := sink.Write(ctx, req)
		if status.Code(err) == codes.AlreadyExists {
			log.Printf("already exists: %s, %s", req.Origin, req.CallId)
			continue
		} else if err != nil {
			return err
		}
	}

	if err := <-errCh; err != nil {
		return err
	}

	return nil
}
