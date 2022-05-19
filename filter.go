package main

import (
	"context"
	"encoding/binary"
	"os"

	"google.golang.org/protobuf/proto"
	"mkm.pub/binlog/reader"
)

type FilterCmd struct {
	CmdCommon

	CallID uint64 `optional:""`
}

func (cmd *FilterCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()
	entries, errCh := reader.Read(ctx, f)

	w := os.Stdout
	for e := range entries {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		if cmd.CallID != 0 {
			if e.CallId != cmd.CallID {
				continue
			}
		}

		if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
			return err
		}

		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	if err := <-errCh; err != nil {
		return err
	}

	return nil
}
