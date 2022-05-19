package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"
)

type StatsCmd struct {
	CmdCommon
}

func (cmd *StatsCmd) Run(cli *Context) error {
	f, err := openFile(cmd.LogInputFile, cli.Follow)
	if err != nil {
		return err
	}
	defer f.Close()

	conversations, err := readConversations(cli, f)
	if err != nil {
		return err
	}
	buckets := [8]time.Duration{
		0,
		time.Millisecond * 50,
		time.Millisecond * 100,
		time.Millisecond * 200,
		time.Millisecond * 500,
		time.Second * 1,
		time.Second * 10,
		time.Second * 100,
	}
	histogramByMethod := map[string][8]int{}
	for _, c := range conversations {
		e := c.ElapsedDuration()
		histogram := histogramByMethod[c.MethodName()]
		for i, b := range buckets {
			if e >= b {
				histogram[i]++
			}
		}
		histogramByMethod[c.MethodName()] = histogram
	}

	var w tabwriter.Writer
	w.Init(os.Stdout, 0, 8, 0, '\t', 0)
	fmt.Fprintf(&w, "Method\t[≥0s]\t[≥0.05s]\t[≥0.1s]\t[≥0.2s]\t[≥0.5s]\t[≥1s]\t[≥10s]\t[≥100s]\t[errors]\n")
	for method, histogram := range histogramByMethod {
		b0 := histogram[0]
		b1 := histogram[1]
		b2 := histogram[2]
		b3 := histogram[3]
		b4 := histogram[4]
		b5 := histogram[5]
		b6 := histogram[6]
		b7 := histogram[7]

		fmt.Fprintf(&w, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n", method, b0, b1, b2, b3, b4, b5, b6, b7)
	}
	w.Flush()

	return nil
}
