package main

import (
	"math"

	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/bb_storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/status"
)

var messageBufferPool = bb_grpc.NewMessageBufferPool(4 << 20)

func init() {
	// SetDefaultBufferPool is only safe during initialization. Install
	// a stable pool here, then configure its contents before creating
	// clients and servers. This also covers the built-in proto codec,
	// whose pool cannot be selected with transport options alone.
	experimental.SetDefaultBufferPool(messageBufferPool)
}

func configureMessageBufferPool(configuration *bb_storage.ApplicationConfiguration) error {
	// The storage-level limit bounds protobuf blobs and CAS batch data,
	// whereas each listener has a separate gRPC receive limit. Include
	// both, and retain support for the default 4 MiB client receive limit.
	maximumSizeBytes := max(int64(4<<20), configuration.MaximumMessageSizeBytes)
	for _, server := range configuration.GrpcServers {
		maximumSizeBytes = max(maximumSizeBytes, server.GetMaximumReceivedMessageSizeBytes())
	}
	if maximumSizeBytes > int64(math.MaxInt) {
		return status.Error(codes.InvalidArgument, "Maximum pooled message size exceeds the platform's addressable size")
	}
	messageBufferPool.Configure(int(maximumSizeBytes))
	return nil
}
