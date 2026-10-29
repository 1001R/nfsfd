package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	portmapPort = 111
	defaultNFS  = 2049
)

type config struct {
	host, export, dir string
	workers           int
	uid, gid          uint32
	mountPort         int
	nfsPort           int
}

func main() {
	var cfg config
	var uid, gid uint
	flag.IntVar(&cfg.workers, "workers", 4, "number of workers, each with its own connection")
	flag.UintVar(&uid, "uid", uint(os.Getuid()), "uid for AUTH_SYS")
	flag.UintVar(&gid, "gid", uint(os.Getgid()), "gid for AUTH_SYS")
	flag.IntVar(&cfg.mountPort, "mountport", 0, "mountd TCP port (default: ask portmapper)")
	flag.IntVar(&cfg.nfsPort, "nfsport", 0, "nfsd TCP port (default: ask portmapper, fall back to 2049)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] hostname export directory\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 3 || cfg.workers < 1 {
		flag.Usage()
		os.Exit(2)
	}
	cfg.host, cfg.export, cfg.dir = flag.Arg(0), flag.Arg(1), flag.Arg(2)
	cfg.uid, cfg.gid = uint32(uid), uint32(gid)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	failed, err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nfsfd: %v\n", err)
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

// nextPort spreads connections across the reserved range so concurrent dials rarely collide.
var nextPort atomic.Uint32

// dialTCP uses a source port below 1024 when running as root, as servers with secure exports require.
func dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	if os.Geteuid() != 0 {
		return d.DialContext(ctx, "tcp", addr)
	}
	const lo, hi = 512, 1023
	const n = hi - lo + 1
	start := nextPort.Add(1)
	for i := range uint32(n) {
		d.LocalAddr = &net.TCPAddr{Port: hi - int((start+i)%n)}
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil || !errors.Is(err, syscall.EADDRINUSE) {
			return conn, err
		}
	}
	return nil, errors.New("no free reserved source port")
}

func dial(ctx context.Context, addr string, port int, cfg config) (*rpcConn, error) {
	conn, err := dialTCP(ctx, net.JoinHostPort(addr, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	return newRPCConn(conn, cfg.uid, cfg.gid), nil
}

func discoverPorts(ctx context.Context, addr string, cfg config) (mountPort, nfsPort int, err error) {
	mountPort, nfsPort = cfg.mountPort, cfg.nfsPort
	if mountPort != 0 && nfsPort != 0 {
		return mountPort, nfsPort, nil
	}
	pm, perr := dial(ctx, addr, portmapPort, cfg)
	if perr == nil {
		defer pm.conn.Close()
		if mountPort == 0 {
			mountPort, perr = portmapGetPort(pm, progMount, 3)
		}
		if perr == nil && nfsPort == 0 {
			nfsPort, _ = portmapGetPort(pm, progNFS, 3)
		}
	}
	if mountPort == 0 {
		return 0, 0, fmt.Errorf("locating mountd via portmapper: %w", perr)
	}
	if nfsPort == 0 {
		nfsPort = defaultNFS
	}
	return mountPort, nfsPort, nil
}

// resolveRoot mounts the export and returns the handle of the directory within it.
func resolveRoot(ctx context.Context, addr string, cfg config) (fh []byte, nfsPort int, err error) {
	mountPort, nfsPort, err := discoverPorts(ctx, addr, cfg)
	if err != nil {
		return nil, 0, err
	}

	mc, err := dial(ctx, addr, mountPort, cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("connecting to mountd: %w", err)
	}
	fh, err = mountExport(mc, cfg.export)
	mc.conn.Close()
	if err != nil {
		return nil, 0, err
	}

	nc, err := dial(ctx, addr, nfsPort, cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("connecting to nfsd: %w", err)
	}
	defer nc.conn.Close()
	for _, name := range strings.Split(path.Clean("/"+cfg.dir), "/") {
		if name == "" {
			continue
		}
		if fh, err = lookup(nc, fh, name); err != nil {
			return nil, 0, fmt.Errorf("lookup %q: %w", name, err)
		}
	}
	return fh, nfsPort, nil
}

func lookupAddrs(ctx context.Context, host string) ([]string, error) {
	all, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var addrs []string
	for _, a := range all {
		if !seen[a] {
			seen[a] = true
			addrs = append(addrs, a)
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses for %s", host)
	}
	return addrs, nil
}

// run reports whether any non-fatal problem occurred.
func run(ctx context.Context, cfg config) (failed bool, err error) {
	addrs, err := lookupAddrs(ctx, cfg.host)
	if err != nil {
		return false, err
	}
	rootFH, nfsPort, err := resolveRoot(ctx, addrs[0], cfg)
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var conns []*rpcConn
	closeAll := func() {
		for _, c := range conns {
			c.conn.Close()
		}
	}
	defer closeAll()
	for i := range cfg.workers {
		c, err := dial(ctx, addrs[i%len(addrs)], nfsPort, cfg)
		if err != nil {
			return false, fmt.Errorf("worker %d: %w", i, err)
		}
		conns = append(conns, c)
	}

	out := newPrinter(os.Stdout)
	var anyFailed atomic.Bool
	work := make(chan item)
	results := make(chan result)

	var wg sync.WaitGroup
	for _, c := range conns {
		w := &worker{conn: c, out: out, failed: &anyFailed}
		wg.Go(func() { w.sender(ctx, work, results) })
		wg.Go(func() { w.receiver(ctx, results) })
	}

	// Main owns the backlog and the count of items not yet answered.
	backlog := []item{{path: path.Clean(cfg.dir), fh: rootFH}}
	outstanding := 1
loop:
	for outstanding > 0 {
		var dispatch chan item // nil disables the send case when the backlog is empty
		var top item
		if n := len(backlog); n > 0 {
			dispatch, top = work, backlog[n-1]
		}
		select {
		case dispatch <- top:
			backlog = backlog[:len(backlog)-1]
		case r := <-results:
			if r.err != nil {
				err = r.err
				break loop
			}
			backlog = append(backlog, r.items...)
			outstanding += len(r.items) - 1
		case <-ctx.Done():
			err = errors.New("interrupted")
			break loop
		}
	}

	cancel()
	closeAll()
	wg.Wait()
	out.flush()
	return anyFailed.Load(), err
}
