package main

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"
	grpc_configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

func TestConfigureMessageBufferPool(t *testing.T) {
	t.Cleanup(func() { messageBufferPool.Configure(4 << 20) })
	for _, tc := range []struct {
		name          string
		configuration *bb_storage.ApplicationConfiguration
		maximum       int
	}{
		{"Default", &bb_storage.ApplicationConfiguration{}, 4 << 20},
		{"StorageLimit", &bb_storage.ApplicationConfiguration{MaximumMessageSizeBytes: 16 << 20}, 16 << 20},
		{"ListenerLimit", &bb_storage.ApplicationConfiguration{
			MaximumMessageSizeBytes: 1 << 20,
			GrpcServers: []*grpc_configuration.ServerConfiguration{
				{MaximumReceivedMessageSizeBytes: 8 << 20},
				{MaximumReceivedMessageSizeBytes: 16 << 20},
			},
		}, 16 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, configureMessageBufferPool(tc.configuration))
			require.Same(t, messageBufferPool, mem.DefaultBufferPool())
			buffer := messageBufferPool.Get(tc.maximum)
			require.Equal(t, tc.maximum+(4<<10), cap(*buffer))
			messageBufferPool.Put(buffer)
			buffer = messageBufferPool.Get(tc.maximum + (4 << 10) + 1)
			require.Equal(t, tc.maximum+(4<<10)+1, cap(*buffer))
			messageBufferPool.Put(buffer)
		})
	}
}

func TestDefaultProtoCodecUsesMessageBufferPool(t *testing.T) {
	// Verify the actual stock codec uses the pool installed by init,
	// without replacing the codec or losing its protobuf behavior.
	codec := encoding.GetCodecV2("proto")
	encoded, err := codec.Marshal(&bytestream.ReadResponse{Data: make([]byte, 64<<10)})
	require.NoError(t, err)
	defer encoded.Free()
	require.Len(t, encoded, 1)
	require.Equal(t, (64<<10)+(4<<10), cap(encoded[0].ReadOnlyData()))
	var decoded bytestream.ReadResponse
	require.NoError(t, codec.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.Data, 64<<10)
}
