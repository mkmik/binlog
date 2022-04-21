package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kong"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"mkm.pub/binlog/reader"
)

type Context struct {
	*CLI
}

type CLI struct {
	ProtoFileNames []string `optional:"" name:"proto" short:"p" help:"Proto files" type:"string"`
	ImportPaths    []string `optional:"" name:"proto_path" short:"I" help:"Import paths" type:"string"`
	DescSet        []string `optional:"" name:"descriptor_set" help:"path to FileDescriptorSet, see protoc -o"`
	CPUProfile     string   `optional:"" name:"cpuprofile" help:"write cpu profile to file"`

	Stats  StatsCmd  `cmd:"" help:"Stats"`
	View   ViewCmd   `cmd:"" help:"View logs"`
	Debug  DebugCmd  `cmd:"" help:"debug"`
	Filter FilterCmd `cmd:"" help:"Create a smaller binlog out of a binlog file"`

	methods map[string]methodTypes
}

type methodTypes struct {
	requestMessageType  protoreflect.FullName
	responseMessageType protoreflect.FullName
}

type CmdCommon struct {
	LogInputFile string `arg:"" name:"log_input_file" help:"Binary log input file"`
}

type StatsCmd struct {
	CmdCommon
}

type ViewCmd struct {
	CmdCommon

	Expand  bool `optional:"" help:"Show message bodies"`
	Headers bool `optional:"" help:"Show headers"`
}

type DebugCmd struct {
	CmdCommon
}

type FilterCmd struct {
	CmdCommon

	CallID uint64 `optional:""`
}

func (c *CLI) AfterApply() error {
	p := &protoparse.Parser{
		ImportPaths: c.ImportPaths,
	}
	fds, err := p.ParseFiles(c.ProtoFileNames...)
	if err != nil {
		return err
	}
	if err := registerFileDescriptors(fds); err != nil {
		return fmt.Errorf("registerFileDescriptors: %w", err)
	}
	if err := registerFileDescriptorSets(c.DescSet); err != nil {
		return fmt.Errorf("registerFileDescriptorSets: %w", err)
	}
	if err := c.registerServices(); err != nil {
		return fmt.Errorf("registerServices: %w", err)
	}
	if c.CPUProfile != "" {
		c.startCPUProfile()
	}
	return nil
}

func (c *CLI) startCPUProfile() {
	f, err := os.Create(c.CPUProfile)
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
}

func (c *CLI) OnExit() {
	pprof.StopCPUProfile()
}

func registerFileDescriptorSets(filenames []string) error {
	for _, descSetFileName := range filenames {
		b, err := ioutil.ReadFile(descSetFileName)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		var fdset descriptorpb.FileDescriptorSet
		if err := proto.Unmarshal(b, &fdset); err != nil {
			return err
		}
		for _, fdp := range fdset.File {
			if err := registerFileDescriptor(fdp); err != nil {
				return fmt.Errorf("fdset %s, file %s: %w", descSetFileName, fdp.GetName(), err)
			}
		}
	}
	return nil
}

func registerFileDescriptors(fds []*desc.FileDescriptor) error {
	for _, fd := range fds {
		if err := registerFileDescriptor(fd.AsFileDescriptorProto()); err != nil {
			return err
		}
	}
	return nil
}

func registerFileDescriptor(fdp *descriptorpb.FileDescriptorProto) error {
	fdr, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	if err != nil {
		return err
	}
	if err := protoregistry.GlobalFiles.RegisterFile(fdr); err != nil {
		return err
	}
	for i := 0; i < fdr.Messages().Len(); i++ {
		mt := dynamicpb.NewMessageType(fdr.Messages().Get(i))
		protoregistry.GlobalTypes.RegisterMessage(mt)
	}
	return nil
}

func (c *CLI) registerServices() error {
	c.methods = map[string]methodTypes{}
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := 0; i < services.Len(); i++ {
			service := services.Get(i)
			methods := service.Methods()
			for j := 0; j < methods.Len(); j++ {
				method := methods.Get(j)
				name := fmt.Sprintf("/%s/%s", service.FullName(), method.Name())
				c.methods[name] = methodTypes{
					requestMessageType:  method.Input().FullName(),
					responseMessageType: method.Output().FullName(),
				}
			}
		}
		return true
	})
	return nil
}

func readConversations(cli *Context, r io.Reader) ([]conversation, error) {
	ctx := context.Background()
	entries, errCh := reader.Read(ctx, r)

	var calls []uint64
	byCall := map[uint64]conversation{}

	for e := range entries {
		conv, found := byCall[e.CallId]
		if !found {
			calls = append(calls, e.CallId)
		}
		switch e.Type {
		case v1.GrpcLogEntry_EVENT_TYPE_CLIENT_HEADER:
			conv.requestHeader = e
		case v1.GrpcLogEntry_EVENT_TYPE_CLIENT_MESSAGE:
			conv.requestMessages = append(conv.requestMessages, e)
		case v1.GrpcLogEntry_EVENT_TYPE_SERVER_HEADER:
			conv.responseHeader = e
		case v1.GrpcLogEntry_EVENT_TYPE_SERVER_MESSAGE:
			conv.responseMessages = append(conv.responseMessages, e)
		case v1.GrpcLogEntry_EVENT_TYPE_SERVER_TRAILER:
			conv.responseTrailer = e
		}

		byCall[e.CallId] = conv
	}
	if err := <-errCh; err != nil {
		return nil, err
	}

	var res []conversation
	for _, i := range calls {
		c := byCall[i]
		res = append(res, c)
	}

	return res, nil
}

func (cmd *ViewCmd) Run(cli *Context) error {
	f, err := os.Open(cmd.LogInputFile)
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
	fmt.Fprintf(&w, "ID\tWhen\tElapsed\tMethod\n")
	for _, c := range conversations {
		// skip conversations that have no client headers
		if c.CallId() == 0 {
			continue
		}
		fmt.Fprintf(&w, "%d\t%s\t%s\t%s\n", c.CallId(), c.Timestamp(), c.Elapsed(), c.MethodName())

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
	}
	w.Flush()

	return nil
}

func (cmd *StatsCmd) Run(cli *Context) error {
	f, err := os.Open(cmd.LogInputFile)
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

func (cmd *DebugCmd) Run(cli *Context) error {
	f, err := os.Open(cmd.LogInputFile)
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

func (cmd *FilterCmd) Run(cli *Context) error {
	f, err := os.Open(cmd.LogInputFile)
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

func renderMetadata(m *v1.Metadata) string {
	var w strings.Builder
	for i, e := range m.GetEntry() {
		fmt.Fprintf(&w, "%q:%q", e.Key, e.Value)
		if i+1 < len(m.GetEntry()) {
			fmt.Fprintf(&w, ", ")
		}
	}
	return w.String()
}

type conversation struct {
	requestHeader    *v1.GrpcLogEntry
	requestMessages  []*v1.GrpcLogEntry
	responseHeader   *v1.GrpcLogEntry
	responseMessages []*v1.GrpcLogEntry
	responseTrailer  *v1.GrpcLogEntry
}

func (c conversation) CallId() uint64 {
	return c.requestHeader.CallId
}

func (c conversation) MethodName() string {
	return c.requestHeader.GetClientHeader().GetMethodName()
}

func (c conversation) Timestamp() string {
	// use same format as /debug/requests (https://cs.opensource.google/go/x/net/+/e204ce36:trace/trace.go;l=888)
	return c.requestHeader.Timestamp.AsTime().Format("2006/01/02 15:04:05.000000")
}

func (c conversation) Elapsed() string {
	// If a conversation lacks a response return 0
	if c.responseTrailer.GetTimestamp() == nil {
		return "(never)"
	}
	return fmt.Sprint(c.ElapsedDuration())
}

func (c conversation) ElapsedDuration() time.Duration {
	return c.responseTrailer.GetTimestamp().AsTime().Sub(c.requestHeader.GetTimestamp().AsTime())
}

func formatMessages(w io.Writer, prefix string, entries []*v1.GrpcLogEntry, messageType string) error {
	for _, m := range entries {
		b, err := formatEntry(m, messageType)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s\t%s", prefix, b); err != nil {
			return err
		}
	}
	return nil
}

func (c conversation) FormatRequest(w io.Writer, ctx *Context) error {
	msgType, err := c.RequestMessageType(ctx)
	if err != nil {
		return err
	}
	return formatMessages(w, "->", c.requestMessages, msgType)
}

func (c conversation) FormatResponse(w io.Writer, ctx *Context) error {
	msgType, err := c.ResponseMessageType(ctx)
	if err != nil {
		return err
	}
	return formatMessages(w, "->", c.responseMessages, msgType)
}

func (c conversation) RequestMessageType(ctx *Context) (string, error) {
	md, ok := ctx.methods[c.MethodName()]
	if !ok {
		return "", fmt.Errorf("cannot find method descriptor for %q", c.MethodName())
	}
	return string(md.requestMessageType), nil
}

func (c conversation) ResponseMessageType(ctx *Context) (string, error) {
	md, ok := ctx.methods[c.MethodName()]
	if !ok {
		return "", fmt.Errorf("cannot find method descriptor for %q", c.MethodName())
	}
	return string(md.responseMessageType), nil
}

func formatEntry(entry *v1.GrpcLogEntry, messageType string) ([]byte, error) {
	raw := entry.GetMessage().GetData()
	msg, err := parseBody(raw, messageType)
	if err != nil {
		return nil, err
	}

	res, err := protojson.MarshalOptions{Multiline: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal dynamic proto: %w", err)
	}
	if entry.PayloadTruncated {
		res = append(res, []byte("...")...)
	}
	return res, nil
}

func parseBody(raw []byte, messageType string) (proto.Message, error) {
	desc, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(messageType))
	if err != nil {
		return nil, fmt.Errorf("cannot find descriptor for %q: %w", messageType, err)
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a message", messageType)
	}
	msg := dynamicpb.NewMessage(msgDesc)
	if err := proto.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("cannot dynamicaly unmarshal raw message: %w", err)
	}
	return msg, nil
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
	cli.OnExit()
}
