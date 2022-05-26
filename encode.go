package main

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type EncodeCmd struct {
	CmdCommon

	CallID uint64 `optional:""`
}

func (cmd *EncodeCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := json.NewDecoder(f)

	conversations := map[uint64]conversation{}

	w := os.Stdout
	for dec.More() {
		var mangled map[string]any
		if err := dec.Decode(&mangled); err != nil {
			return err
		}
		if _, hasMessage := mangled["message"]; hasMessage {
			msg := mangled["message"].(map[string]any)
			if _, hasDecoded := msg["decoded"]; hasDecoded {
				var callId uint64
				if err := json.Unmarshal([]byte(mangled["callId"].(string)), &callId); err != nil {
					return err
				}
				conv, found := conversations[callId]
				if !found {
					return fmt.Errorf("cannot find headers for call ID %d", callId)
				}

				decoded, err := json.Marshal(msg["decoded"])
				if err != nil {
					return err
				}
				messageType, err := conv.RequestMessageType(cli)
				if err != nil {
					return err
				}
				dataMessage, err := newDynamicMessage(messageType)
				if err != nil {
					return err
				}
				if err := protojson.Unmarshal([]byte(decoded), dataMessage); err != nil {
					return err
				}
				delete(msg, "decoded")
				encoded, err := proto.Marshal(dataMessage)
				if err != nil {
					return err
				}
				msg["data"] = base64.StdEncoding.EncodeToString(encoded)
			}

		}
		mb, err := json.Marshal(&mangled)
		if err != nil {
			return nil
		}
		var e v1.GrpcLogEntry
		if err := protojson.Unmarshal(mb, &e); err != nil {
			return err
		}
		conv := conversations[e.CallId]
		conv.Record(&e)
		conversations[e.CallId] = conv

		b, err := proto.Marshal(&e)
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
