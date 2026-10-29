package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"
)

const (
	maxInflight = 32 // must be a power of two; xid low bits carry the slot index
	maxRecord   = 64 << 20
	rpcVersion  = 2
	authNone    = 0
	authSys     = 1
	msgCall     = 0
	msgReply    = 1
	syncTimeout = 30 * time.Second
)

// pending is the state a receiver needs to interpret a reply.
type pending struct {
	xid  uint32
	path string
	fh   []byte
}

type reply struct {
	p    *pending
	body []byte
	err  error // RPC-level failure for this request; the connection is still usable
}

// rpcConn is a TCP ONC RPC client. send must only be called from one goroutine
// and recv from one goroutine; call is for unpipelined use during setup.
type rpcConn struct {
	conn    net.Conn
	r       *bufio.Reader
	cred    []byte // encoded credential and verifier
	free    chan int
	slots   [maxInflight]atomic.Pointer[pending]
	counter uint32
}

func newRPCConn(conn net.Conn, uid, gid uint32) *rpcConn {
	host, _ := os.Hostname()
	var body xdrWriter
	body.u32(uint32(time.Now().Unix()))
	body.str(host)
	body.u32(uid)
	body.u32(gid)
	body.u32(0) // no supplementary gids

	var w xdrWriter
	w.u32(authSys)
	w.opaque(body.b)
	w.u32(authNone)
	w.u32(0)

	c := &rpcConn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 256<<10),
		cred: w.b,
		free: make(chan int, maxInflight),
	}
	for i := range maxInflight {
		c.free <- i
	}
	return c
}

func (c *rpcConn) nextXID() uint32 {
	// Keep the counter in 1..2^27-1 so xid is never 0 and the shift can't overflow.
	c.counter = c.counter%(1<<27-1) + 1
	return c.counter << 5
}

func (c *rpcConn) encodeCall(xid, prog, vers, proc uint32, args []byte) []byte {
	w := xdrWriter{b: make([]byte, 4, 4+24+len(c.cred)+len(args))}
	w.u32(xid)
	w.u32(msgCall)
	w.u32(rpcVersion)
	w.u32(prog)
	w.u32(vers)
	w.u32(proc)
	w.b = append(w.b, c.cred...)
	w.b = append(w.b, args...)
	binary.BigEndian.PutUint32(w.b, uint32(len(w.b)-4)|1<<31)
	return w.b
}

func (c *rpcConn) readRecord() ([]byte, error) {
	var rec []byte
	for {
		var h [4]byte
		if _, err := io.ReadFull(c.r, h[:]); err != nil {
			return nil, err
		}
		v := binary.BigEndian.Uint32(h[:])
		n := int(v & 0x7fffffff)
		if len(rec)+n > maxRecord {
			return nil, fmt.Errorf("rpc record too large")
		}
		start := len(rec)
		rec = append(rec, make([]byte, n)...)
		if _, err := io.ReadFull(c.r, rec[start:]); err != nil {
			return nil, err
		}
		if v&(1<<31) != 0 {
			return rec, nil
		}
	}
}

// parseReply returns xid 0 if the header is too short to contain one.
func parseReply(rec []byte) (xid uint32, body []byte, err error) {
	r := &xdrReader{b: rec}
	xid = r.u32()
	if r.err != nil {
		return 0, nil, r.err
	}
	if r.u32() != msgReply {
		return xid, nil, fmt.Errorf("rpc: not a reply")
	}
	if stat := r.u32(); stat != 0 {
		return xid, nil, fmt.Errorf("rpc: call denied (reply_stat %d)", stat)
	}
	r.u32() // verifier flavor
	r.opaque(400)
	accept := r.u32()
	if r.err != nil {
		return xid, nil, r.err
	}
	if accept != 0 {
		return xid, nil, fmt.Errorf("rpc: call not executed (accept_stat %d)", accept)
	}
	return xid, r.b, nil
}

// call performs one synchronous request/response exchange.
func (c *rpcConn) call(prog, vers, proc uint32, args []byte) ([]byte, error) {
	xid := c.nextXID()
	c.conn.SetDeadline(time.Now().Add(syncTimeout))
	if _, err := c.conn.Write(c.encodeCall(xid, prog, vers, proc, args)); err != nil {
		return nil, err
	}
	for {
		rec, err := c.readRecord()
		if err != nil {
			return nil, err
		}
		got, body, rerr := parseReply(rec)
		if got == xid {
			return body, rerr
		}
	}
}

// send issues an NFS request, blocking while maxInflight requests are outstanding.
func (c *rpcConn) send(ctx context.Context, proc uint32, args []byte, path string, fh []byte) error {
	var idx int
	select {
	case idx = <-c.free:
	case <-ctx.Done():
		return ctx.Err()
	}
	xid := c.nextXID() | uint32(idx)
	c.slots[idx].Store(&pending{xid: xid, path: path, fh: fh})
	_, err := c.conn.Write(c.encodeCall(xid, progNFS, 3, proc, args))
	return err
}

// recv reads one reply. A non-nil error means the connection is unusable.
func (c *rpcConn) recv() (reply, error) {
	rec, err := c.readRecord()
	if err != nil {
		return reply{}, err
	}
	xid, body, rerr := parseReply(rec)
	if xid == 0 {
		return reply{}, fmt.Errorf("malformed rpc reply: %w", rerr)
	}
	idx := int(xid & (maxInflight - 1))
	p := c.slots[idx].Load()
	if p == nil || p.xid != xid {
		return reply{}, fmt.Errorf("rpc reply with unexpected xid %#x", xid)
	}
	c.slots[idx].Store(nil)
	c.free <- idx
	return reply{p: p, body: body, err: rerr}, nil
}
