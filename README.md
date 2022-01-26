# binlog

Viewer for https://github.com/grpc/grpc/blob/master/doc/binary-logging.md


## Demo:

```console
$ ./binlog --proto ~/tmp/gorpc/helloworld/helloworld.proto stats /tmp/grpcgo_binarylog_1037680716.txt
2022/01/26 17:22:23 last entry truncated, ignoring
Method								[≥0s]	[≥0.05s][≥0.1s]	[≥0.2s]	[≥0.5s]	[≥1s]	[≥10s]	[≥100s]	[errors]
/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo	21	0	0	0	0	0	0	0
/helloworld.Greeter/SayHello					20	0	0	0	0	0	0	0
$ ./binlog --proto ~/tmp/gorpc/helloworld/helloworld.proto view /tmp/grpcgo_binarylog_1037680716.txt | head
2022/01/26 17:22:29 last entry truncated, ignoring
When				Elapsed	Method
2022/01/26 12:03:01.895903	1.072ms	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
2022/01/26 12:03:01.897807	41µs	/helloworld.Greeter/SayHello
2022/01/26 12:03:02.032776	43µs	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
2022/01/26 12:03:02.033513	41µs	/helloworld.Greeter/SayHello
2022/01/26 12:03:02.167865	27µs	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
2022/01/26 12:03:02.168554	33µs	/helloworld.Greeter/SayHello
2022/01/26 12:03:02.299392	25µs	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
2022/01/26 12:03:02.300090	26µs	/helloworld.Greeter/SayHello
2022/01/26 12:03:02.434406	29µs	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
$ ./binlog --proto ~/tmp/gorpc/helloworld/helloworld.proto view /tmp/grpcgo_binarylog_1037680716.txt --expand | head
2022/01/26 17:22:34 last entry truncated, ignoring
When				Elapsed	Method
2022/01/26 12:03:01.895903	1.072ms	/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo
->					cannot find method descriptor for "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
<-					cannot find method descriptor for "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"

2022/01/26 12:03:01.897807	41µs	/helloworld.Greeter/SayHello
->					{"name":"foo"}
<-					{"message":"Hello foo from "}
```