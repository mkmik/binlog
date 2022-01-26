package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"text/tabwriter"

	"github.com/alecthomas/kong"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

type Context struct {
	*CLI
}

type CLI struct {
	ProtoFileNames []string `optional:"" name:"proto" short:"p" help:"Proto files" type:"path"`
	ImportPaths    []string `optional:"" name:"proto_path" short:"I" help:"Import paths" type:"path"`

	Stats StatsCmd `cmd:"" help:"Stats"`
	View  ViewCmd  `cmd:"" help:"View logs"`

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

	Expand bool `optional:"" help:"Show message bodies"`
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
		return err
	}
	if err := c.registerServices(); err != nil {
		return err
	}
	return nil
}

func registerFileDescriptors(fds []*desc.FileDescriptor) error {
	for _, fd := range fds {
		fdp := fd.AsFileDescriptorProto()
		fdr, err := protodesc.NewFile(fdp, nil)
		if err != nil {
			return err
		}
		protoregistry.GlobalFiles.RegisterFile(fdr)
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
				method := methods.Get(i)
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

func readEntries(r io.Reader) ([]v1.GrpcLogEntry, error) {
	var res []v1.GrpcLogEntry
	for {
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(r, hdr); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("error reading count: %w", err)
		}

		size := binary.BigEndian.Uint32(hdr)

		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("last entry truncated, ignoring")
				return res, nil
			}
			return nil, fmt.Errorf("error reading body: %#v %w", err, err)
		}
		var entry v1.GrpcLogEntry
		if err := proto.Unmarshal(body, &entry); err != nil {
			return nil, err
		}
		res = append(res, entry)
	}
	return res, nil
}

func readConversations(cli *Context, r io.Reader) ([]conversation, error) {
	entries, err := readEntries(r)
	if err != nil {
		return nil, err
	}

	var calls []uint64
	byCall := map[uint64]conversation{}

	for _, e := range entries {
		conv, found := byCall[e.CallId]
		if !found {
			calls = append(calls, e.CallId)
		}
		switch e.Type {
		case v1.GrpcLogEntry_EVENT_TYPE_CLIENT_HEADER:
			conv.requestHeader = e
		case v1.GrpcLogEntry_EVENT_TYPE_CLIENT_MESSAGE:
			conv.requestMessage = e
		case v1.GrpcLogEntry_EVENT_TYPE_SERVER_MESSAGE:
			conv.responseMessage = e
		}

		byCall[e.CallId] = conv
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
	fmt.Fprintf(&w, "When\tElapsed\tMethod\n")
	for _, c := range conversations {
		fmt.Fprintf(&w, "%s\t%s\t%s\n", c.Timestamp(), c.Elapsed(), c.MethodName())

		if cmd.Expand {
			req, err := c.FormatRequest(cli)
			if err != nil {
				fmt.Fprintf(&w, "->\t\t%v\n", err)
			} else {
				fmt.Fprintf(&w, "->\t\t%s\n", req)
			}

			res, err := c.FormatResponse(cli)
			if err != nil {
				fmt.Fprintf(&w, "<-\t\t%v\n", err)
			} else {
				fmt.Fprintf(&w, "<-\t\t%s\n", res)
			}
			fmt.Fprintln(&w)
		}
	}
	w.Flush()

	return nil
}

type conversation struct {
	requestHeader   v1.GrpcLogEntry
	requestMessage  v1.GrpcLogEntry
	responseMessage v1.GrpcLogEntry
}

func (c conversation) MethodName() string {
	return c.requestHeader.GetClientHeader().MethodName
}

func (c conversation) Timestamp() string {
	// use same format as /debug/requests (https://cs.opensource.google/go/x/net/+/e204ce36:trace/trace.go;l=888)
	return c.requestMessage.Timestamp.AsTime().Format("2006/01/02 15:04:05.000000")
}

func (c conversation) Elapsed() string {
	// If a conversation lacks a response return 0
	if c.responseMessage.Timestamp == nil {
		return "(never)"
	}
	d := c.responseMessage.Timestamp.AsTime().Sub(c.requestMessage.Timestamp.AsTime())
	return fmt.Sprint(d)
}

func (c conversation) FormatRequest(ctx *Context) ([]byte, error) {
	msgType, err := c.RequestMessageType(ctx)
	if err != nil {
		return nil, err
	}
	return formatEntry(&c.requestMessage, msgType)
}

func (c conversation) FormatResponse(ctx *Context) ([]byte, error) {
	msgType, err := c.ResponseMessageType(ctx)
	if err != nil {
		return nil, err
	}
	return formatEntry(&c.responseMessage, msgType)
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

	res, err := protojson.Marshal(msg)
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
}
