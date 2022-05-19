package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"google.golang.org/grpc/codes"
)

type ViewCmd struct {
	CmdCommon

	Expand        bool `optional:"" help:"Show message bodies"`
	Headers       bool `optional:"" help:"Show headers"`
	StatusMessage bool `optional:"" help:"Show status message"`
}

func (cmd *ViewCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	conversations, err := readConversations(cli, f)
	if err != nil {
		return err
	}

	var w tabwriter.Writer
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(&w, "ID\tWhen\tElapsed\tMethod\tStatus\n")
	for _, c := range conversations {
		// skip conversations that have no client headers
		if c.CallId() == 0 {
			continue
		}

		statusCode := codes.Code(c.responseTrailer.GetTrailer().GetStatusCode())
		fmt.Fprintf(&w, "%d\t%s\t%s\t%s\t%s\n", c.CallId(), c.Timestamp(), c.Elapsed(), c.MethodName(), statusCode)

		if cmd.Headers {
			if m := c.requestHeader.GetClientHeader().GetMetadata(); len(m.GetEntry()) > 0 {
				fmt.Fprintf(&w, "->{h}\t%s\n", renderMetadata(m))
			}
			if m := c.responseHeader.GetServerHeader().GetMetadata(); len(m.GetEntry()) > 0 {
				fmt.Fprintf(&w, "<-{h}\t%s\n", renderMetadata(m))
			}
			if m := c.responseTrailer.GetTrailer().GetMetadata(); len(m.GetEntry()) > 0 {
				fmt.Fprintf(&w, "<-{t}\t%s\n", renderMetadata(m))
			}
		}
		if cmd.Expand {
			if err := c.FormatRequest(&w, cli); err != nil {
				fmt.Fprintf(&w, "->\t%v\n", err)
			}
			if err := c.FormatResponse(&w, cli); err != nil {
				fmt.Fprintf(&w, "<-\t%v\n", err)
			}
			fmt.Fprintln(&w)
		}
		if cmd.StatusMessage {
			if t := c.responseTrailer.GetTrailer(); t != nil {
				fmt.Fprintf(&w, "<-{s}\t%s\n", t.StatusMessage)
			}
		}
	}
	w.Flush()

	return nil
}
