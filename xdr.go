package main

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	errShort   = errors.New("xdr: short buffer")
	errTooLong = errors.New("xdr: opaque too long")
)

type xdrWriter struct{ b []byte }

func (w *xdrWriter) u32(v uint32) { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *xdrWriter) u64(v uint64) { w.b = binary.BigEndian.AppendUint64(w.b, v) }

func (w *xdrWriter) fixed(p []byte) {
	w.b = append(w.b, p...)
	for n := len(p); n%4 != 0; n++ {
		w.b = append(w.b, 0)
	}
}

func (w *xdrWriter) opaque(p []byte) {
	w.u32(uint32(len(p)))
	w.fixed(p)
}

func (w *xdrWriter) str(s string) { w.opaque([]byte(s)) }

// xdrReader records the first error and returns zero values afterwards.
type xdrReader struct {
	b   []byte
	err error
}

func (r *xdrReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.err = errShort
		return nil
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p
}

func (r *xdrReader) u32() uint32 {
	p := r.take(4)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint32(p)
}

func (r *xdrReader) u64() uint64 {
	p := r.take(8)
	if p == nil {
		return 0
	}
	return binary.BigEndian.Uint64(p)
}

func (r *xdrReader) bool() bool { return r.u32() != 0 }

func (r *xdrReader) fixed(n int) []byte {
	p := r.take(n)
	r.take((4 - n%4) % 4)
	return p
}

// opaque returns a copy so callers don't pin the whole reply buffer.
func (r *xdrReader) opaque(max int) []byte {
	n := r.u32()
	if r.err == nil && uint64(n) > uint64(max) {
		r.err = errTooLong
	}
	p := r.fixed(int(n))
	if p == nil {
		return nil
	}
	return bytes.Clone(p)
}
