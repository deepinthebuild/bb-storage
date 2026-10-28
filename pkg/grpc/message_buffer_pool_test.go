package grpc_test

import (
	"math"
	"strings"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestMessageBufferPoolWireSizes(t *testing.T) {
	pool := bb_grpc.NewMessageBufferPool(4 << 20)
	for _, tc := range []struct {
		name     string
		message  proto.Message
		capacity int
	}{
		{
			name:     "ByteStreamRead64KiB",
			message:  &bytestream.ReadResponse{Data: make([]byte, 64<<10)},
			capacity: (64 << 10) + (4 << 10),
		},
		{
			name: "ByteStreamWrite64KiB",
			message: &bytestream.WriteRequest{
				ResourceName: "instance/uploads/00000000-0000-0000-0000-000000000000/blobs/" + strings.Repeat("a", 64) + "/1048576",
				WriteOffset:  1 << 20,
				Data:         make([]byte, 64<<10),
			},
			capacity: (64 << 10) + (4 << 10),
		},
		{
			name:     "ByteStreamRead1MiB",
			message:  &bytestream.ReadResponse{Data: make([]byte, 1<<20)},
			capacity: (1 << 20) + (4 << 10),
		},
		{
			name: "CASBatchRead4MiB",
			message: &remoteexecution.BatchReadBlobsResponse{
				Responses: []*remoteexecution.BatchReadBlobsResponse_Response{{
					Digest: &remoteexecution.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 4 << 20},
					Data:   make([]byte, 4<<20),
				}},
			},
			capacity: (4 << 20) + (4 << 10),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			size := proto.Size(tc.message)
			buffer := pool.Get(size)
			defer pool.Put(buffer)
			require.Len(t, *buffer, size)
			require.Equal(t, tc.capacity, cap(*buffer))
			wire, err := proto.MarshalOptions{}.MarshalAppend((*buffer)[:0], tc.message)
			require.NoError(t, err)
			require.Same(t, &(*buffer)[0], &wire[0])
			decoded := tc.message.ProtoReflect().New().Interface()
			require.NoError(t, proto.Unmarshal(wire, decoded))
			require.True(t, proto.Equal(tc.message, decoded))
		})
	}
}

func TestMessageBufferPoolTransportSizes(t *testing.T) {
	pool := bb_grpc.NewMessageBufferPool(4 << 20)
	for _, size := range []int{0, 1, 1024, 4 << 10, 16 << 10, 32 << 10} {
		buffer := pool.Get(size)
		require.Len(t, *buffer, size)
		require.Equal(t, max(4<<10, size), cap(*buffer))
		pool.Put(buffer)
	}
}

func TestMessageBufferPoolMaximumSizeOverflow(t *testing.T) {
	// An unusually large configured maximum must not overflow while
	// constructing classes, even though no enormous buffer is acquired.
	pool := bb_grpc.NewMessageBufferPool(math.MaxInt)
	buffer := pool.Get((64 << 10) + 4)
	defer pool.Put(buffer)
	require.Equal(t, (64<<10)+(4<<10), cap(*buffer))
}

func TestMessageBufferPoolOversized(t *testing.T) {
	pool := bb_grpc.NewMessageBufferPool(1 << 20)
	// Larger RPCs still work. Returning a short prefix must not retain
	// their large backing allocation or promote a smaller size class.
	buffer := pool.Get(3 << 20)
	require.Len(t, *buffer, 3<<20)
	require.Equal(t, 3<<20, cap(*buffer))
	*buffer = (*buffer)[:1]
	pool.Put(buffer)
	buffer = pool.Get((64 << 10) + 4)
	defer pool.Put(buffer)
	require.Equal(t, (64<<10)+(4<<10), cap(*buffer))
}

func TestMessageBufferPoolReconfigure(t *testing.T) {
	pool := bb_grpc.NewMessageBufferPool(4 << 20)
	buffer := pool.Get(3 << 20)
	// Reconfiguring while a reference-counted gRPC buffer is in flight
	// must preserve its bytes and allow it to be freed normally.
	(*buffer)[0] = 42
	ref := mem.NewBuffer(buffer, pool)
	pool.Configure(1 << 20)
	require.Equal(t, byte(42), ref.ReadOnlyData()[0])
	ref.Free()
	buffer = pool.Get(2 << 20)
	require.Equal(t, 2<<20, cap(*buffer))
	pool.Put(buffer)

	pool.Configure(3 << 20)
	buffer = pool.Get(3 << 20)
	require.Equal(t, (3<<20)+(4<<10), cap(*buffer))
	pool.Put(buffer)
}

func TestMessageBufferPoolConcurrentReconfigure(t *testing.T) {
	pool := bb_grpc.NewMessageBufferPool(4 << 20)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				buffer := pool.Get((64 << 10) + 4)
				if len(*buffer) != (64<<10)+4 {
					t.Errorf("Unexpected buffer length: %d", len(*buffer))
				}
				(*buffer)[0] = 42
				pool.Put(buffer)
			}
		})
	}
	for range 20 {
		pool.Configure(1 << 20)
		pool.Configure(4 << 20)
	}
	wg.Wait()
}

func BenchmarkMessageBufferPool(b *testing.B) {
	for _, tc := range []struct {
		name    string
		message proto.Message
	}{
		{"ByteStreamRead64KiB", &bytestream.ReadResponse{Data: make([]byte, 64<<10)}},
		{"ByteStreamWrite64KiB", &bytestream.WriteRequest{
			ResourceName: "instance/uploads/00000000-0000-0000-0000-000000000000/blobs/" + strings.Repeat("a", 64) + "/1048576",
			WriteOffset:  1 << 20,
			Data:         make([]byte, 64<<10),
		}},
		{"ByteStreamRead1MiB", &bytestream.ReadResponse{Data: make([]byte, 1<<20)}},
		{"CASBatchRead4MiB", &remoteexecution.BatchReadBlobsResponse{
			Responses: []*remoteexecution.BatchReadBlobsResponse_Response{{
				Digest: &remoteexecution.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 4 << 20},
				Data:   make([]byte, 4<<20),
			}},
		}},
	} {
		for _, pc := range []struct {
			name string
			pool mem.BufferPool
		}{
			{"Default", mem.DefaultBufferPool()},
			{"Bazel", bb_grpc.NewMessageBufferPool(4 << 20)},
		} {
			b.Run(tc.name+"/"+pc.name, func(b *testing.B) {
				size := proto.Size(tc.message)
				buffer := pc.pool.Get(size)
				capacity := cap(*buffer)
				pc.pool.Put(buffer)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					size := proto.Size(tc.message)
					buffer := pc.pool.Get(size)
					_, err := (proto.MarshalOptions{UseCachedSize: true}).MarshalAppend((*buffer)[:0], tc.message)
					pc.pool.Put(buffer)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(capacity), "capacity-B")
			})
		}
	}
}
