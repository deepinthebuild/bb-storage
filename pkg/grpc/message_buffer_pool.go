package grpc

import (
	"math"
	"sync/atomic"

	"google.golang.org/grpc/mem"
)

const messageBufferPoolHeadroom = 4 << 10

type messageBufferPoolState struct {
	pool             mem.BufferPool
	maximumSizeBytes int
}

// MessageBufferPool pools gRPC wire buffers using size classes suited to
// remote execution traffic. Configure may be called while buffers are in
// use. A buffer returned after reconfiguration is subject to the new limit.
// Instances must be created using NewMessageBufferPool and must not be copied.
type MessageBufferPool struct {
	state atomic.Pointer[messageBufferPoolState]
}

// NewMessageBufferPool creates a buffer pool for the given maximum message
// payload size. It leaves room for protobuf envelopes around blob data.
// Larger messages remain supported, but their buffers are not retained.
func NewMessageBufferPool(maximumMessageSizeBytes int) *MessageBufferPool {
	p := &MessageBufferPool{}
	p.Configure(maximumMessageSizeBytes)
	return p
}

// Configure replaces the pool's size classes and retention ceiling. A
// nonpositive maximum selects gRPC's default receive limit of 4 MiB.
// Reconfiguration releases the old pools for garbage collection; it does
// not invalidate outstanding buffers or change any RPC message limits.
func (p *MessageBufferPool) Configure(maximumMessageSizeBytes int) {
	if maximumMessageSizeBytes <= 0 {
		maximumMessageSizeBytes = 4 << 20
	}
	maximumSizeBytes := maximumMessageSizeBytes + min(messageBufferPoolHeadroom, math.MaxInt-maximumMessageSizeBytes)

	// Preserve compact classes for metadata, HTTP/2 DATA frames, and
	// transport buffers. gRPC itself bypasses pooling for small messages.
	var sizes []int
	for _, size := range []int{4 << 10, 16 << 10, 32 << 10} {
		if size < maximumSizeBytes {
			sizes = append(sizes, size)
		}
	}
	// Both the ByteStream server and CAS clients send 64 KiB chunks.
	// A ReadResponse or WriteRequest containing a full chunk needs more
	// than 64 KiB. Headroom prevents these envelopes from promoting
	// buffers into the next power-of-two class. Retain intermediate
	// classes for clients using larger chunks and for CAS batches.
	for size := 64 << 10; size < maximumMessageSizeBytes; {
		sizes = append(sizes, size+messageBufferPoolHeadroom)
		if size > maximumMessageSizeBytes/2 {
			break
		}
		size *= 2
	}
	sizes = append(sizes, maximumSizeBytes)
	p.state.Store(&messageBufferPoolState{
		pool:             mem.NewTieredBufferPool(sizes...),
		maximumSizeBytes: maximumSizeBytes,
	})
}

// Get obtains a buffer of the requested length. Requests above the
// retention ceiling allocate an exact-sized buffer without using a pool.
func (p *MessageBufferPool) Get(length int) *[]byte {
	state := p.state.Load()
	if length > state.maximumSizeBytes {
		return mem.NopBufferPool{}.Get(length)
	}
	return state.pool.Get(length)
}

// Put releases a buffer. Capacity, rather than length, determines whether
// it can be retained, so a short view cannot hide an oversized allocation.
func (p *MessageBufferPool) Put(buffer *[]byte) {
	state := p.state.Load()
	if cap(*buffer) <= state.maximumSizeBytes {
		state.pool.Put(buffer)
	}
}
