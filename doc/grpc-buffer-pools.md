# gRPC wire buffer pooling

`bb_storage` installs a process-wide buffer pool for the stock grpc-go
protobuf codec and client transports. The server factory explicitly uses
the current default pool, because grpc-go v1.81.1 captures its server
default during package initialization.

The pool is installed in `init`, as required by
`experimental.SetDefaultBufferPool`. After reading configuration, but
before applying global configuration or creating clients and servers,
the daemon configures that same pool object. This avoids replacing gRPC's
global allocator while background goroutines may be using it. Other
Buildbarn binaries keep their existing default allocator.

## Size classes

The pool retains 4 KiB, 16 KiB, and 32 KiB classes for smaller messages and
HTTP/2 frames. Starting at 64 KiB, classes double in payload size, with
4 KiB of room for protobuf tags, lengths, digests, resource names, and
other envelope fields. In particular, a 64 KiB ByteStream chunk from
either `bb_storage` or its CAS clients fits in a 68 KiB buffer. grpc-go's
default pool assigns these encoded messages to its 1 MiB class.

The largest payload class is the maximum of:

- 4 MiB, the default gRPC client/server receive limit;
- the storage `maximum_message_size_bytes` setting;
- every listener's `maximum_received_message_size_bytes` setting.

The largest retained capacity includes the same 4 KiB headroom. This
ceiling controls reuse, not RPC acceptance. Variable protobuf envelopes,
custom service configurations, compression, or relayed messages can
exceed it; larger buffers are still allocated normally and are discarded
when released. Capacity is checked even when the returned slice is short.
The underlying pools use `sync.Pool` through grpc-go: they do not impose a
limit on the total memory held across concurrent RPCs, and GC can discard
idle buffers.

Pooling does not extend to decoded protobuf object graphs. The stock
codec, message limits, protobuf ownership, and gRPC reference-counted
buffer lifetime remain in effect. Messages below grpc-go's pooling
threshold still use its ordinary allocation path.

## Validation and benchmarking

Run the repository's Bazel tests, which supply its generated mocks and
patched dependencies:

```sh
bazel test //pkg/grpc:grpc_test //cmd/bb_storage:bb_storage_test
```

The focused pool tests and serialization benchmark can also run without
the Bazel-generated test mocks:

```sh
go test -race ./pkg/grpc/message_buffer_pool_test.go
go test -race cmd/bb_storage/message_buffer_pool.go cmd/bb_storage/message_buffer_pool_test.go
go test ./pkg/grpc/message_buffer_pool_test.go -run '^$' \
  -bench BenchmarkMessageBufferPool -benchmem -count=3
```

The benchmark exercises `proto.Size`, buffer acquisition, marshaling into
the acquired buffer, and buffer return for actual ByteStream and CAS
message types. It reports the buffer's slice capacity as `capacity-B`;
this excludes allocator rounding and pool bookkeeping. Both default and
custom pools can report zero allocations after warmup. This benchmark
therefore measures capacity and buffer clearing/marshaling cost, not
end-to-end RPC throughput or decoded-object allocation savings.

An initial local run on linux/amd64, Go 1.27.3, grpc-go v1.81.1, with three
300 ms samples per case produced:

| Case | Default capacity | Custom capacity | Default ns/op, median | Custom ns/op, median |
| --- | ---: | ---: | ---: | ---: |
| ByteStream Read, 64 KiB data | 1,048,576 | 69,632 | 14,092 | 2,014 |
| ByteStream Write, 64 KiB data | 1,048,576 | 69,632 | 15,866 | 2,282 |
| ByteStream Read, 1 MiB data | 1,052,672 | 1,052,672 | 69,093 | 69,453 |
| CAS BatchRead, 4 MiB data | 4,198,400 | 4,198,400 | 270,751 | 244,535 |

All cases reported zero allocations per operation after warmup. These
results are preliminary microbenchmarks, not production performance
claims. Follow-up validation should replay representative Bazel traffic
and compare `alloc_space`, `alloc_objects`, GC CPU, in-use heap, throughput,
and tail latency under realistic concurrency and GC pressure.
