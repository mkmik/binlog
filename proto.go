package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

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
