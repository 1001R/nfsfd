package main

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestModeString(t *testing.T) {
	tests := []struct {
		ftype, mode uint32
		want        string
	}{
		{ftypeDir, 0o755, "drwxr-xr-x"},
		{ftypeReg, 0o644, "-rw-r--r--"},
		{ftypeLnk, 0o777, "lrwxrwxrwx"},
		{ftypeReg, 0o4755, "-rwsr-xr-x"},
		{ftypeReg, 0o4644, "-rwSr--r--"},
		{ftypeDir, 0o2755, "drwxr-sr-x"},
		{ftypeDir, 0o1777, "drwxrwxrwt"},
		{ftypeDir, 0o1776, "drwxrwxrwT"},
		{ftypeFifo, 0o600, "prw-------"},
		{ftypeSock, 0o600, "srw-------"},
		{ftypeBlk, 0o660, "brw-rw----"},
		{ftypeChr, 0o660, "crw-rw----"},
	}
	for _, tt := range tests {
		if got := modeString(tt.ftype, tt.mode); got != tt.want {
			t.Errorf("modeString(%d, %o) = %q, want %q", tt.ftype, tt.mode, got, tt.want)
		}
	}
}

func TestAppendEntry(t *testing.T) {
	p := newPrinter(io.Discard)
	p.users.Store(uint32(1000), "alice")
	p.groups.Store(uint32(1000), "staff")
	a := &fattr{ftype: ftypeReg, mode: 0o644, nlink: 1, uid: 1000, gid: 1000, size: 123, mtimeSec: 1700000000}
	got := string(p.appendEntry(nil, "/d/f", a))
	want := "-rw-r--r--   1 alice    staff           123 2023-11-14T22:13:20Z /d/f\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	a = &fattr{ftype: ftypeDir, mode: 0o755, nlink: 2, uid: 1000, gid: 1000, size: 4096, mtimeSec: 1700000000}
	got = string(p.appendEntry(nil, "/d", a))
	want = "drwxr-xr-x   2 alice    staff             0 2023-11-14T22:13:20Z /d\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestXDRRoundTrip(t *testing.T) {
	var w xdrWriter
	w.u32(7)
	w.u64(1 << 40)
	w.opaque([]byte("abcde"))
	w.str("xy")
	r := &xdrReader{b: w.b}
	if r.u32() != 7 || r.u64() != 1<<40 || string(r.opaque(16)) != "abcde" || string(r.opaque(16)) != "xy" {
		t.Fatal("round trip mismatch")
	}
	if r.err != nil || len(r.b) != 0 {
		t.Fatalf("err=%v remaining=%d", r.err, len(r.b))
	}
	r = &xdrReader{b: w.b[:10]}
	r.u32()
	r.u64()
	if r.err == nil {
		t.Fatal("expected short buffer error")
	}
}

func putAttr(w *xdrWriter, ftype, mode uint32, size uint64) {
	w.u32(1) // attributes follow
	w.u32(ftype)
	w.u32(mode)
	w.u32(1)
	w.u32(1000)
	w.u32(1000)
	w.u64(size)
	w.u64(size)
	w.u64(0)
	w.u64(0)
	w.u64(0)
	w.u64(0)
	w.u32(1700000000)
	w.u32(5)
	w.u64(0)
}

func TestParseReaddirplus(t *testing.T) {
	var w xdrWriter
	w.u32(0) // NFS3_OK
	w.u32(0) // no dir attributes
	w.fixed([]byte("verfverf"))
	w.u32(1)
	w.u64(1)
	w.str("sub")
	w.u64(11)
	putAttr(&w, ftypeDir, 0o755, 4096)
	w.u32(1)
	w.opaque([]byte("handle"))
	w.u32(1)
	w.u64(2)
	w.str("noattr")
	w.u64(12)
	w.u32(0)
	w.u32(0)
	w.u32(0) // end of list
	w.u32(1) // eof

	pg, err := parseReaddirplus(w.b)
	if err != nil {
		t.Fatal(err)
	}
	if !pg.eof || string(pg.verf[:]) != "verfverf" || len(pg.entries) != 2 {
		t.Fatalf("unexpected page %+v", pg)
	}
	e := pg.entries[0]
	if e.name != "sub" || e.cookie != 11 || e.attr == nil || e.attr.ftype != ftypeDir || string(e.fh) != "handle" {
		t.Fatalf("unexpected entry %+v", e)
	}
	if e := pg.entries[1]; e.attr != nil || e.fh != nil {
		t.Fatalf("unexpected entry %+v", e)
	}

	var bad xdrWriter
	bad.u32(2)
	bad.u32(0)
	if _, err := parseReaddirplus(bad.b); err != nfsStatus(2) {
		t.Fatalf("got %v", err)
	}
}

func serverReply(xid uint32, body []byte) []byte {
	var w xdrWriter
	w.b = make([]byte, 4)
	w.u32(xid)
	w.u32(msgReply)
	w.u32(0)
	w.u32(authNone)
	w.u32(0)
	w.u32(0)
	w.b = append(w.b, body...)
	w.b[0] = 0x80 | byte((len(w.b)-4)>>24)
	w.b[1] = byte((len(w.b) - 4) >> 16)
	w.b[2] = byte((len(w.b) - 4) >> 8)
	w.b[3] = byte(len(w.b) - 4)
	return w.b
}

func TestInflightLimitAndOutOfOrder(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srvConn := make(chan *rpcConn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			srvConn <- newRPCConn(c, 0, 0)
		}
	}()

	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client := newRPCConn(cc, 0, 0)
	defer cc.Close()
	srv := <-srvConn
	defer srv.conn.Close()

	const total = maxInflight + 8
	var sent atomic.Int32
	go func() {
		for i := range total {
			if err := client.send(context.Background(), procReaddirplus, nil, "p", []byte{byte(i)}); err != nil {
				return
			}
			sent.Add(1)
		}
	}()

	xids := make([]uint32, 0, total)
	read := func() {
		rec, err := srv.readRecord()
		if err != nil {
			t.Fatal(err)
		}
		xids = append(xids, uint32(rec[0])<<24|uint32(rec[1])<<16|uint32(rec[2])<<8|uint32(rec[3]))
	}
	for range maxInflight {
		read()
	}
	time.Sleep(100 * time.Millisecond)
	if n := sent.Load(); n != maxInflight {
		t.Fatalf("sent %d requests without any reply, want %d", n, maxInflight)
	}

	// Answer newest first to exercise out-of-order matching.
	answered := 0
	for answered < total {
		for len(xids) > 0 {
			x := xids[len(xids)-1]
			xids = xids[:len(xids)-1]
			if _, err := srv.conn.Write(serverReply(x, []byte{0, 0, 0, 0})); err != nil {
				t.Fatal(err)
			}
			rep, err := client.recv()
			if err != nil || rep.err != nil {
				t.Fatalf("recv: %v / %v", err, rep.err)
			}
			if rep.p.xid != x {
				t.Fatalf("matched xid %#x, want %#x", rep.p.xid, x)
			}
			answered++
			if answered+len(xids) < total && len(xids) < 1 {
				read()
			}
		}
	}
	if answered != total {
		t.Fatalf("answered %d", answered)
	}
}
