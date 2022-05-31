package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	v1 "mkm.pub/binlog/proto"
)

type RecvCmd struct {
	Listen string `required:"" help:"listen address of gRPC server"`
}

type server struct {
	v1.UnimplementedLogSinkServiceServer
}

func (s *server) Write(ctx context.Context, req *v1.WriteRequest) (*v1.WriteResponse, error) {
	e := req.Entry
	fmt.Printf("%d\t%s\t%s\n", e.CallId, e.GetType(), e.GetClientHeader().GetMethodName())
	return &v1.WriteResponse{}, nil
}

func (cmd *RecvCmd) Run(cli *Context) error {
	listener, err := net.Listen("tcp", cmd.Listen)
	if err != nil {
		return err
	}

	srv := grpc.NewServer()
	reflection.Register(srv)
	v1.RegisterLogSinkServiceServer(srv, &server{})

	log.Printf("Serving gRPC at %q", cmd.Listen)
	return srv.Serve(listener)
}
