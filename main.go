package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/mkmik/stringlist"
	v1 "google.golang.org/grpc/binarylog/grpc_binarylog_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
			if err == io.EOF {
				break
			}
			return nil, err
		}

		size := binary.BigEndian.Uint32(hdr)

		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		var entry v1.GrpcLogEntry
		if err := proto.Unmarshal(body, &entry); err != nil {
			return nil, err
		}
		res = append(res, entry)
	}
	return res, nil
}

func mainE(filename string, protoFileName string, importPaths []string) error {
	p := &protoparse.Parser{
		ImportPaths: importPaths,
	}
	fds, err := p.ParseFiles(protoFileName)
	if err != nil {
		return err
	}
	_ = fds

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	entries, err := readEntries(f)
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
		fmt.Println(c.RawRequest())
		fmt.Println(c.RawResponse())

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
