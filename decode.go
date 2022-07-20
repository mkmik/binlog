package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"mkm.pub/binlog/reader"
)

type DecodeCmd struct {
	CmdCommon

	CallID uint64 `optional:""`

	// if true, decodes `data` into `decoded` to help with editing.
	Expand bool `optional:""`
}

func (cmd *DecodeCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()
	entries, errCh := reader.Read(ctx, f)

	conversations := map[uint64]conversation{}

	w := os.Stdout
	for e := range entries {
		if cmd.CallID != 0 {
			if e.CallId != cmd.CallID {
				continue
			}
		}
		conv := conversations[e.CallId]
		conv.Record(e)
		conversations[e.CallId] = conv

		res, err := protojson.MarshalOptions{Multiline: true}.Marshal(e)
		if err != nil {
			return fmt.Errorf("cannot marshal dynamic proto: %w", err)
		}
		if cmd.Expand && e.Type == v1.GrpcLogEntry_EVENT_TYPE_CLIENT_MESSAGE {
			var unstructured map[string]any
			if err := json.Unmarshal(res, &unstructured); err != nil {
				return err
			}
			if true {
				delete(unstructured["message"].(map[string]any), "data")

				msgType, err := conv.RequestMessageType(cli)
				if err != nil {
					return err
				}
				decoded, err := formatEntry(e, msgType)
				if err != nil {
					return fmt.Errorf("decoding as %q: %w", msgType, err)
				}

				unstructured["message"].(map[string]any)["decoded"] = json.RawMessage(decoded)
			}
			res, err = json.MarshalIndent(unstructured, "", "  ")
			if err != nil {
				return err
			}
		}

		fmt.Fprintf(w, "%s\n", res)
	}

	if err := <-errCh; err != nil {
		return err
	}

	return nil
}
