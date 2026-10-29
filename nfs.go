package main

import (
	"fmt"
)

const (
	progPortmap = 100000
	progNFS     = 100003
	progMount   = 100005

	ipprotoTCP = 6

	procGetPort     = 3 // portmap v2
	procMount       = 1 // mount v3 MNT
	procLookup      = 3 // nfs v3
	procReaddirplus = 17

	maxFH   = 64
	maxName = 1024

	dirCount = 32 << 10
	maxCount = 32 << 10
)

const (
	ftypeReg  = 1
	ftypeDir  = 2
	ftypeBlk  = 3
	ftypeChr  = 4
	ftypeLnk  = 5
	ftypeSock = 6
	ftypeFifo = 7
)

type nfsStatus uint32

var nfsStatusNames = map[nfsStatus]string{
	1:     "NFS3ERR_PERM",
	2:     "NFS3ERR_NOENT",
	5:     "NFS3ERR_IO",
	13:    "NFS3ERR_ACCES",
	20:    "NFS3ERR_NOTDIR",
	22:    "NFS3ERR_INVAL",
	70:    "NFS3ERR_STALE",
	10001: "NFS3ERR_BADHANDLE",
	10003: "NFS3ERR_BAD_COOKIE",
	10005: "NFS3ERR_TOOSMALL",
	10006: "NFS3ERR_SERVERFAULT",
	10008: "NFS3ERR_JUKEBOX",
}

func (s nfsStatus) Error() string {
	if n, ok := nfsStatusNames[s]; ok {
		return n
	}
	return fmt.Sprintf("nfs status %d", uint32(s))
}

type fattr struct {
	ftype, mode, nlink, uid, gid uint32
	size                         uint64
	mtimeSec, mtimeNsec          uint32
}

func (r *xdrReader) fattr() *fattr {
	f := &fattr{}
	f.ftype = r.u32()
	f.mode = r.u32()
	f.nlink = r.u32()
	f.uid = r.u32()
	f.gid = r.u32()
	f.size = r.u64()
	r.u64() // used
	r.u64() // rdev
	r.u64() // fsid
	r.u64() // fileid
	r.u64() // atime
	f.mtimeSec = r.u32()
	f.mtimeNsec = r.u32()
	r.u64() // ctime
	return f
}

func (r *xdrReader) postOpAttr() *fattr {
	if r.bool() {
		return r.fattr()
	}
	return nil
}

type dirEntry struct {
	name   string
	cookie uint64
	attr   *fattr // nil if the server omitted attributes
	fh     []byte // nil if the server omitted the handle
}

type dirPage struct {
	verf    [8]byte
	entries []dirEntry
	eof     bool
}

func encodeReaddirplus(it item) []byte {
	var w xdrWriter
	w.opaque(it.fh)
	w.u64(it.cookie)
	w.fixed(it.verf[:])
	w.u32(dirCount)
	w.u32(maxCount)
	return w.b
}

func parseReaddirplus(body []byte) (*dirPage, error) {
	r := &xdrReader{b: body}
	if st := nfsStatus(r.u32()); r.err == nil && st != 0 {
		return nil, st
	}
	r.postOpAttr()
	pg := &dirPage{}
	copy(pg.verf[:], r.fixed(8))
	for r.bool() {
		var e dirEntry
		r.u64() // fileid
		e.name = string(r.opaque(maxName))
		e.cookie = r.u64()
		e.attr = r.postOpAttr()
		if r.bool() {
			if fh := r.opaque(maxFH); len(fh) > 0 {
				e.fh = fh
			}
		}
		pg.entries = append(pg.entries, e)
	}
	pg.eof = r.bool()
	if r.err != nil {
		return nil, r.err
	}
	return pg, nil
}

func portmapGetPort(c *rpcConn, prog, vers uint32) (int, error) {
	var w xdrWriter
	w.u32(prog)
	w.u32(vers)
	w.u32(ipprotoTCP)
	w.u32(0)
	body, err := c.call(progPortmap, 2, procGetPort, w.b)
	if err != nil {
		return 0, err
	}
	r := &xdrReader{b: body}
	port := r.u32()
	if r.err != nil {
		return 0, r.err
	}
	if port == 0 {
		return 0, fmt.Errorf("program %d version %d not registered with portmapper", prog, vers)
	}
	return int(port), nil
}

func mountExport(c *rpcConn, export string) ([]byte, error) {
	var w xdrWriter
	w.str(export)
	body, err := c.call(progMount, 3, procMount, w.b)
	if err != nil {
		return nil, err
	}
	r := &xdrReader{b: body}
	st := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	if st != 0 {
		return nil, fmt.Errorf("mount of %q failed: status %d", export, st)
	}
	fh := r.opaque(maxFH)
	return fh, r.err
}

func lookup(c *rpcConn, dir []byte, name string) ([]byte, error) {
	var w xdrWriter
	w.opaque(dir)
	w.str(name)
	body, err := c.call(progNFS, 3, procLookup, w.b)
	if err != nil {
		return nil, err
	}
	r := &xdrReader{b: body}
	st := nfsStatus(r.u32())
	if r.err != nil {
		return nil, r.err
	}
	if st != 0 {
		return nil, st
	}
	fh := r.opaque(maxFH)
	return fh, r.err
}
