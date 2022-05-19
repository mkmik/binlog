package main

import (
	"context"
	"fmt"
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
	for id, c := range conversations {
		fmt.Fprintf(w, "%d\t%s\t%s\n", id, time.Now().Format("2006/01/02 15:04:05.000000"), c.MethodName())
		if err := replayConversation(ctx, conn, &c); err != nil {
			return err
		}
		if cmd.Expand {
			if err := c.FormatResponse(w, cli); err != nil {
				log.Printf("render response: %v", err)
			}
		}
		st := status.Convert(err)
		fmt.Fprintf(w, "<-{s}\t%s\t%s\n", st.Code(), st.Message())
	}
	return nil
}

func replayConversation(ctx context.Context, conn *grpc.ClientConn, c *conversation) error {
	if got, want := len(c.requestMessages), 1; got != want {
		return fmt.Errorf("only unary messages are support (got %d client messages)", got)
	}
	c.responseMessages = nil

	var res []byte
	err := conn.Invoke(ctx, c.MethodName(), c.requestMessages[0].GetMessage().Data, &res, grpc.ForceCodec(&noopCodec{}))
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

	return nil
}

// A noopCodec just passes around already encoded grpc payloads.
type noopCodec struct{}

func (d *noopCodec) Marshal(v interface{}) ([]byte, error)      { return v.([]byte), nil }
func (d *noopCodec) Unmarshal(data []byte, v interface{}) error { *v.(*[]byte) = data; return nil }
func (d *noopCodec) Name() string                               { return "noop" }
