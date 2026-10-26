package worker

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const (
	dialTimeout = 5 * time.Second
	// callTimeout bounds one RPC, so a coordinator that vanishes without
	// closing the connection cannot hang the worker. It must exceed the
	// coordinator's long-poll window.
	callTimeout = 10 * time.Second
)

// client is a net/rpc client that reconnects after transport errors.
type client struct {
	addr string

	mu sync.Mutex
	rc *rpc.Client
}

func (c *client) call(ctx context.Context, method string, args, reply any) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	rc, err := c.get(ctx)
	if err != nil {
		return err
	}
	call := rc.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil && !isServerError(call.Error) {
			c.drop(rc)
		}
		return call.Error
	case <-ctx.Done():
		// Closing the connection fails the pending call, and waiting for
		// that guarantees net/rpc no longer writes into reply.
		c.drop(rc)
		<-call.Done
		return ctx.Err()
	}
}

func (c *client) get(ctx context.Context) (*rpc.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rc != nil {
		return c.rc, nil
	}
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	c.rc = rpc.NewClient(conn)
	return c.rc, nil
}

// drop closes rc if it is still the current connection, so the next call
// dials afresh.
func (c *client) drop(rc *rpc.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rc == rc {
		c.rc.Close()
		c.rc = nil
	}
}

func (c *client) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rc != nil {
		c.rc.Close()
		c.rc = nil
	}
}

// isServerError reports whether err came from the coordinator's handler
// rather than from the transport.
func isServerError(err error) bool {
	var se rpc.ServerError
	return errors.As(err, &se)
}
