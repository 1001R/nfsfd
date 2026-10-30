# nfsfd

Parallel recursive directory lister for NFSv3 servers. It walks a directory on an export using pipelined `READDIRPLUS` requests and prints one CSV line per entry, which makes it suitable for building file inventories of large shares.

Written in Go using only the standard library; the ONC RPC, XDR, portmapper, MOUNT and NFSv3 protocol code is included.

## Build

```sh
go build -o nfsfd .
go test -race ./...
```

Requires Go 1.27 (see `mise.toml`).

## Usage

```
nfsfd [flags] hostname export directory
```

| Argument | Meaning |
|---|---|
| `hostname` | NFS server. If it resolves to several addresses, workers use them round-robin. |
| `export` | Exported path to mount, as listed by `showmount -e`. |
| `directory` | Directory inside the export to list (`/` or `.` for the export root). |

| Flag | Default | Meaning |
|---|---|---|
| `-workers` | `4` | Number of workers, each with its own TCP connection. |
| `-uid` | current uid | uid sent in the `AUTH_SYS` credential. |
| `-gid` | current gid | gid sent in the `AUTH_SYS` credential. |
| `-mountport` | portmapper | mountd TCP port. |
| `-nfsport` | portmapper, then 2049 | nfsd TCP port. |
| `-version` | | Print version and commit hash, then exit. |

Example:

```sh
nfsfd -workers 8 nfs.example.com /export/data projects | gzip > inventory.csv.gz
```

Warnings go to stderr. The exit status is 1 if any entry was skipped or a fatal error occurred.

## Output

One line per entry, with no header. The starting directory itself is not printed.

```
mode,owner,group,size,mtime,"path"
drwxr-xr-x,alice,staff,0,2023-11-14T22:13:20Z,"projects/src"
-rw-r--r--,alice,staff,123,2023-11-14T22:13:20Z,"projects/src/main.go"
```

- `mode`: file type and permissions as in `ls -l` (`-`, `d`, `l`, `b`, `c`, `s`, `p`; setuid, setgid and sticky shown as `s`/`S`/`t`/`T`).
- `owner`, `group`: names resolved on the machine running `nfsfd`, falling back to the numeric id.
- `size`: bytes for regular files, `0` for everything else.
- `mtime`: modification time in UTC, ISO 8601.
- `path`: `directory` joined with the entry names, as a quoted CSV field with embedded `"` doubled.

Output order is not deterministic because directories are listed concurrently.

## Behaviour

- Ports: mountd and nfsd are located through the portmapper on port 111. If nfsd is not registered, port 2049 is used. Both can be overridden with flags.
- Authentication: `AUTH_SYS` only (no Kerberos). Access is checked by the server against `-uid` and `-gid`.
- Source ports: when running as root, connections use a source port below 1024, as required by exports without the `insecure` option.
- Concurrency: each worker has one sender and one receiver goroutine. A connection has at most 64 requests in flight (`slotBits` in `rpc.go`).
- Entries are skipped with a warning when the server returns no attributes, when a directory has no file handle (it is printed but not descended into), or when the name is not valid UTF-8.
- Symbolic links are listed but not followed, and their targets are not shown.
- A connection error aborts the run with exit status 1; Ctrl-C stops it cleanly and flushes the output collected so far.
- Only NFSv3 over TCP is supported.

## Loading the output into DuckDB

DuckDB reads the gzip-compressed CSV directly and can write Parquet:

```sql
COPY (
  SELECT * FROM read_csv('inventory.csv.gz',
    header = false,
    columns = {
      'mode': 'VARCHAR', 'owner': 'VARCHAR', 'grp': 'VARCHAR',
      'size': 'UBIGINT', 'mtime': 'TIMESTAMP', 'path': 'VARCHAR'
    },
    timestampformat = '%Y-%m-%dT%H:%M:%SZ')
) TO 'inventory.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000);
```

A single `.gz` file is decompressed on one thread. For very large shares, split the output into several files and read them with a glob.

## Files

| File | Purpose |
|---|---|
| `main.go` | Flags, host resolution, port discovery, mount and root lookup, work scheduling. |
| `worker.go` | Per-connection sender and receiver goroutines, reply handling. |
| `rpc.go` | Pipelined ONC RPC client over TCP with record marking and `AUTH_SYS`. |
| `nfs.go` | Portmapper, MOUNT and NFSv3 (`LOOKUP`, `READDIRPLUS`) encoding and decoding. |
| `xdr.go` | XDR reader and writer. |
| `output.go` | CSV formatting, mode string, user and group name cache. |
