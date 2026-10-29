package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"unicode/utf8"
)

// item is one READDIRPLUS request; cookie 0 starts a directory.
type item struct {
	path   string
	fh     []byte
	cookie uint64
	verf   [8]byte
}

// result reports one processed reply: the follow-up items it produced, or a fatal error.
type result struct {
	items []item
	err   error
}

type worker struct {
	conn   *rpcConn
	out    *printer
	failed *atomic.Bool
}

func (w *worker) warn(p string, err error) {
	w.failed.Store(true)
	fmt.Fprintf(os.Stderr, "nfsfd: %s: %v\n", p, err)
}

func (w *worker) report(ctx context.Context, results chan<- result, r result) bool {
	select {
	case results <- r:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *worker) sender(ctx context.Context, work <-chan item, results chan<- result) {
	for {
		var it item
		select {
		case it = <-work:
		case <-ctx.Done():
			return
		}
		if err := w.conn.send(ctx, procReaddirplus, encodeReaddirplus(it), it.path, it.fh); err != nil {
			if ctx.Err() == nil {
				w.report(ctx, results, result{err: fmt.Errorf("send: %w", err)})
			}
			return
		}
	}
}

func (w *worker) receiver(ctx context.Context, results chan<- result) {
	for {
		r, err := w.conn.recv()
		if err != nil {
			if ctx.Err() == nil {
				w.report(ctx, results, result{err: fmt.Errorf("receive: %w", err)})
			}
			return
		}
		if !w.report(ctx, results, result{items: w.handle(r)}) {
			return
		}
	}
}

func (w *worker) handle(r reply) []item {
	p := r.p
	if r.err != nil {
		w.warn(p.path, r.err)
		return nil
	}
	pg, err := parseReaddirplus(r.body)
	if err != nil {
		w.warn(p.path, err)
		return nil
	}

	var next []item
	var lines []byte
	defer func() { w.out.write(lines) }()
	for _, e := range pg.entries {
		if e.name == "." || e.name == ".." {
			continue
		}
		if !utf8.ValidString(e.name) {
			w.warn(p.path, fmt.Errorf("invalid UTF-8, skipping entry %q", e.name))
			continue
		}
		full := path.Join(p.path, e.name)
		if e.attr == nil {
			w.warn(full, fmt.Errorf("server returned no attributes, skipping"))
			continue
		}
		lines = w.out.appendEntry(lines, full, e.attr)
		if e.attr.ftype != ftypeDir {
			continue
		}
		if e.fh == nil {
			w.warn(full, fmt.Errorf("server returned no file handle, not descending"))
			continue
		}
		next = append(next, item{path: full, fh: e.fh})
	}

	if !pg.eof {
		if len(pg.entries) == 0 {
			w.warn(p.path, fmt.Errorf("empty reply without eof, listing truncated"))
			return next
		}
		next = append(next, item{
			path:   p.path,
			fh:     p.fh,
			cookie: pg.entries[len(pg.entries)-1].cookie,
			verf:   pg.verf,
		})
	}
	return next
}
