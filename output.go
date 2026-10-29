package main

import (
	"bufio"
	"fmt"
	"io"
	"os/user"
	"strconv"
	"sync"
	"time"
)

type printer struct {
	mu     sync.Mutex
	w      *bufio.Writer
	users  sync.Map // uint32 -> string
	groups sync.Map // uint32 -> string
}

func newPrinter(w io.Writer) *printer {
	return &printer{w: bufio.NewWriter(w)}
}

func (p *printer) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.w.Flush()
}

func (p *printer) entry(path string, a *fattr) {
	var size uint64
	if a.ftype == ftypeReg {
		size = a.size
	}
	mtime := time.Unix(int64(a.mtimeSec), int64(a.mtimeNsec)).UTC().Format("2006-01-02T15:04:05Z")
	line := fmt.Sprintf("%s %3d %-8s %-8s %10d %s %s\n",
		modeString(a.ftype, a.mode), a.nlink,
		lookupName(&p.users, a.uid, func(id string) (string, error) {
			u, err := user.LookupId(id)
			if err != nil {
				return "", err
			}
			return u.Username, nil
		}),
		lookupName(&p.groups, a.gid, func(id string) (string, error) {
			g, err := user.LookupGroupId(id)
			if err != nil {
				return "", err
			}
			return g.Name, nil
		}),
		size, mtime, path)

	p.mu.Lock()
	p.w.WriteString(line)
	p.mu.Unlock()
}

// lookupName caches resolved names and falls back to the numeric id.
func lookupName(cache *sync.Map, id uint32, resolve func(string) (string, error)) string {
	if v, ok := cache.Load(id); ok {
		return v.(string)
	}
	name, err := resolve(strconv.FormatUint(uint64(id), 10))
	if err != nil || name == "" {
		name = strconv.FormatUint(uint64(id), 10)
	}
	cache.Store(id, name)
	return name
}

func modeString(ftype, mode uint32) string {
	b := []byte("----------")
	switch ftype {
	case ftypeDir:
		b[0] = 'd'
	case ftypeBlk:
		b[0] = 'b'
	case ftypeChr:
		b[0] = 'c'
	case ftypeLnk:
		b[0] = 'l'
	case ftypeSock:
		b[0] = 's'
	case ftypeFifo:
		b[0] = 'p'
	case ftypeReg:
	default:
		b[0] = '?'
	}
	const rwx = "rwxrwxrwx"
	for i := range 9 {
		if mode&(1<<(8-i)) != 0 {
			b[1+i] = rwx[i]
		}
	}
	special := func(bit uint32, pos int, set, setNoExec byte) {
		if mode&bit == 0 {
			return
		}
		if b[pos] == 'x' {
			b[pos] = set
		} else {
			b[pos] = setNoExec
		}
	}
	special(0o4000, 3, 's', 'S')
	special(0o2000, 6, 's', 'S')
	special(0o1000, 9, 't', 'T')
	return string(b)
}
