package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "mkm.pub/binlog/proto"
	"mkm.pub/binlog/reader"
)

type SendCmd struct {
	CmdCommon

	Target string `required:"" help:"address of a target gRPC server"`
	Origin string `required:""`
	Prefix string `required:"" help:"call-id prefix"`
}

func (cmd *SendCmd) Run(cli *Context) error {
	ctx := context.Background()
	client, err := grpc.DialContext(ctx, cmd.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {

	}
	sink := v1.NewLogSinkServiceClient(client)

	entries, errCh := reader.ReadFile(ctx, cmd.LogInputFile, cli.Follow)

	for e := range entries {
		_, err := sink.Write(ctx, &v1.WriteRequest{
			Origin: cmd.Origin,
			CallId: fmt.Sprintf("%s-%d", cmd.Prefix, e.CallId),
			Entry:  e,
		})
		if err != nil {
			return err
		}
	}

	if err := <-errCh; err != nil {
		return err
	}

	return nil
}
