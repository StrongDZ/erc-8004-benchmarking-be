package rabbitmq

import (
	"context"
	"testing"
	"time"
)

func TestDetachedHandlerContext_activeParentReturnsSame(t *testing.T) {
	parent := context.Background()
	hctx, cancel := DetachedHandlerContext(parent, 45*time.Second)
	defer cancel()
	if hctx != parent {
		t.Fatalf("expected same context pointer as parent, got different context")
	}
}

func TestDetachedHandlerContext_canceledParentReturnsNewWithDeadline(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	hctx, hcancel := DetachedHandlerContext(parent, 45*time.Second)
	defer hcancel()

	if hctx == parent {
		t.Fatal("expected new context when parent is canceled")
	}
	if err := hctx.Err(); err != nil {
		t.Fatalf("expected nil Err immediately after detach, got %v", err)
	}
	deadline, ok := hctx.Deadline()
	if !ok {
		t.Fatal("expected deadline on detached handler context")
	}
	if !deadline.After(time.Now()) {
		t.Fatalf("expected deadline in the future, got %v", deadline)
	}
}
