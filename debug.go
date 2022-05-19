package main

import (
	"context"
	"fmt"

	"mkm.pub/binlog/reader"
)

type DebugCmd struct {
	CmdCommon
}

func (cmd *DebugCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()
	entries, errCh := reader.Read(ctx, f)

	for e := range entries {
		fmt.Printf("%d\t%s\t%s\n", e.CallId, e.GetType(), e.GetClientHeader().GetMethodName())
	}

	if err := <-errCh; err != nil {
		return err
	}

	return nil
}
