package main

import (
	"bufio"
	"io"
	"os/user"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"
)

type printer struct {
	mu     sync.Mutex
	w      *bufio.Writer
	users  sync.Map // uint32 -> string
	groups sync.Map // uint32 -> string
}

func newPrinter(w io.Writer) *printer {
	return &printer{w: bufio.NewWriterSize(w, 64<<10)}
}

// write emits a batch of complete lines atomically.
func (p *printer) write(b []byte) {
	if len(b) == 0 {
		return
	}
	p.mu.Lock()
	p.w.Write(b)
	p.mu.Unlock()
}

func (p *printer) flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.w.Flush()
}

// appendEntry appends one find -ls style line to dst.
func (p *printer) appendEntry(dst []byte, path string, a *fattr) []byte {
	var size uint64
	if a.ftype == ftypeReg {
		size = a.size
	}
	dst = appendMode(dst, a.ftype, a.mode)
	dst = append(dst, ' ')
	dst = appendPadLeft(dst, strconv.FormatUint(uint64(a.nlink), 10), 3)
	dst = append(dst, ' ')
	dst = appendPadRight(dst, lookupName(&p.users, a.uid, userName), 8)
	dst = append(dst, ' ')
	dst = appendPadRight(dst, lookupName(&p.groups, a.gid, groupName), 8)
	dst = append(dst, ' ')
	dst = appendPadLeft(dst, strconv.FormatUint(size, 10), 10)
	dst = append(dst, ' ')
	dst = time.Unix(int64(a.mtimeSec), int64(a.mtimeNsec)).UTC().AppendFormat(dst, "2006-01-02T15:04:05Z")
	dst = append(dst, ' ')
	dst = append(dst, path...)
	return append(dst, '\n')
}

func userName(id string) (string, error) {
	u, err := user.LookupId(id)
	if err != nil {
		return "", err
	}
	return u.Username, nil
}

func groupName(id string) (string, error) {
	g, err := user.LookupGroupId(id)
	if err != nil {
		return "", err
	}
	return g.Name, nil
}

func appendPadLeft(dst []byte, s string, width int) []byte {
	for n := utf8.RuneCountInString(s); n < width; n++ {
		dst = append(dst, ' ')
	}
	return append(dst, s...)
}

func appendPadRight(dst []byte, s string, width int) []byte {
	dst = append(dst, s...)
	for n := utf8.RuneCountInString(s); n < width; n++ {
		dst = append(dst, ' ')
	}
	return dst
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
	return string(appendMode(nil, ftype, mode))
}

func appendMode(dst []byte, ftype, mode uint32) []byte {
	var buf [10]byte
	b := buf[:]
	copy(b, "----------")
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
	return append(dst, b...)
}
