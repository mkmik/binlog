package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ReplayCmd struct {
	CmdCommon

	CallID uint64 `optional:""`
	Target string `required:"" help:"address of a target gRPC server"`

	Expand bool `optional:"" help:"Show reply message bodies"`
}

func (cmd *ReplayCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, cmd.Target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	conversations, err := readConversations(cli, f)
	if err != nil {
		return err
	}

	w := os.Stdout
	fmt.Fprintf(w, "ID\tWhen\tElapsed\tMethod\tStatus\tDetails\n")
	for _, c := range conversations {
		// skip conversations that have no client headers
		if c.CallId() == 0 {
			continue
		}

		if cmd.CallID != 0 {
			if c.CallId() != cmd.CallID {
				continue
			}
		}
		start := time.Now()
		fmt.Fprintf(w, "%d\t%s\t\t%s\n", c.CallId(), start.Format("2006/01/02 15:04:05.000000"), c.MethodName())
		rpcerr := replayConversation(ctx, conn, &c)
		if cmd.Expand {
			if err := c.FormatResponse(w, cli); err != nil {
				log.Printf("render response: %v", err)
			}
		}
		end := time.Now()
		elapsed := end.Sub(start)
		st := status.Convert(rpcerr)
		fmt.Fprintf(w, "<-{s}\t%s\t%s\t\t%s\t%s\n", end.Format("2006/01/02 15:04:05.000000"), elapsed, st.Code(), st.Message())
	}
	return nil
}

func replayConversation(ctx context.Context, conn *grpc.ClientConn, c *conversation) error {
	desc := &grpc.StreamDesc{
		ClientStreams: len(c.requestMessages) != 0,
		ServerStreams: len(c.responseMessages) != 0,
	}
	stream, err := conn.NewStream(ctx, desc, c.MethodName(), grpc.ForceCodec(&noopCodec{}))
	if err != nil {
		return err
	}

	for _, msg := range c.requestMessages {
		if err := stream.SendMsg(msg.GetMessage().Data); err != nil {
			return err
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	c.responseMessages = nil
	for {
		var res []byte
		err := stream.RecvMsg(&res)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rm := &grpc_binarylog_v1.GrpcLogEntry{
			Timestamp: timestamppb.Now(),
			Payload: &grpc_binarylog_v1.GrpcLogEntry_Message{
				Message: &grpc_binarylog_v1.Message{
					Length: uint32(len(res)),
					Data:   res,
				},
			},
		}
		c.responseMessages = append(c.responseMessages, rm)
	}

	return nil
}

// A noopCodec just passes around already encoded grpc payloads.
type noopCodec struct{}

func (d *noopCodec) Marshal(v any) ([]byte, error)      { return v.([]byte), nil }
func (d *noopCodec) Unmarshal(data []byte, v any) error { *v.(*[]byte) = data; return nil }
func (d *noopCodec) Name() string                       { return "noop" }
