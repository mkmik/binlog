package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/mkmik/stringlist"
	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	protoFileName = flag.String("proto", "", "Proto schema files")
	importPaths   = stringlist.Flag("I", "Proto include path")
)

func init() {
	flag.Usage = usage
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

func mainE(filename string, protoFileName string, importPaths []string) error {
	p := &protoparse.Parser{
		ImportPaths: importPaths,
	}
	fds, err := p.ParseFiles(protoFileName)
	if err != nil {
		return err
	}
	if err := registerFileDescriptors(fds); err != nil {
		return err
	}

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	entries, err := readEntries(f)
	log.Printf("Got %d entries", len(entries))
	if err != nil {
		return err
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

	for _, i := range calls {
		c := byCall[i]
		fmt.Printf("%s %s:\n", c.Timestamp(), c.MethodName())
		req, err := c.FormatRequest()
		if err != nil {
			fmt.Printf("cannot format request: %v\n", err)
		} else {
			fmt.Printf("-> %s\n", req)
		}

		res, err := c.FormatResponse()
		if err != nil {
			fmt.Printf("cannot format response: %v\n", err)
		} else {
			fmt.Printf("<- %s\n", res)
		}

		fmt.Println()
	}

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
	return protojson.Format(c.requestMessage.Timestamp)
}

func (c conversation) RawRequest() []byte {
	return c.requestMessage.GetMessage().GetData()
}

func (c conversation) RawResponse() []byte {
	return c.responseMessage.GetMessage().GetData()
}

func (c conversation) FormatRequest() ([]byte, error) {
	msgType, err := c.RequestMessageType()
	if err != nil {
		return nil, err
	}
	return Format(c.RawRequest(), msgType)
}

func (c conversation) FormatResponse() ([]byte, error) {
	msgType, err := c.ResponseMessageType()
	if err != nil {
		return nil, err
	}
	return Format(c.RawResponse(), msgType)
}

func (c conversation) RequestMessageType() (string, error) {
	if false {
		md, err := methodDescriptor(c.MethodName())
		if err != nil {
			return "", fmt.Errorf("cannot find method descriptor for %q: %w", c.MethodName(), err)
		}
		_ = md
	}
	return "helloworld.HelloRequest", nil
}

func (c conversation) ResponseMessageType() (string, error) {
	return "helloworld.HelloReply", nil
}

func methodDescriptor(methodName string) (protoreflect.MethodDescriptor, error) {
	c := strings.SplitN(methodName, "/", 3)
	service, method := c[1], c[2]

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		log.Printf("FILE DESCRIPTOR SERVICES: %#v", fd.Services())
		return true
	})

	return nil, fmt.Errorf("TODO %q, %q", service, method)
}

func Format(raw []byte, messageType string) ([]byte, error) {
	msg, err := Parse(raw, messageType)
	if err != nil {
		return nil, err
	}

	res, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("cannot marshal dynamic proto: %w", err)
	}
	return res, nil
}

func Parse(raw []byte, messageType string) (proto.Message, error) {
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

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <filename>\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	flag.Parse()

	if flag.NArg() < 1 {
		usage()
	}
	if *protoFileName == "" {
		usage()
	}

	if err := mainE(flag.Arg(0), *protoFileName, *importPaths); err != nil {
		log.Fatal(err)
	}
}
