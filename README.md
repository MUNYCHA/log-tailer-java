# log-tailer

**log-tailer** is a lightweight, **Java 8-compatible** application that tails log files and collects server storage metrics, publishing everything to **Apache Kafka**.

It provides:
- Continuous tailing of multiple log files → Kafka topics
- Periodic disk usage snapshots → Kafka topic
- Server name attached to log events; system and server identity attached to storage snapshots

---

## Requirements

| Requirement | Version |
|-------------|---------|
| Java        | 8 (1.8) or higher |
| Maven       | 3.6+    |
| Kafka       | Reachable broker  |
| OS          | Linux (paths and tailing are Linux-oriented) |

Verify Java version:
```bash
java -version
```

---

## Build

```bash
mvn clean package
```

Produces a fat JAR at:

```
target/log-tailer.jar
```

---

## Run

There are three supported ways to launch the app, depending on where you are:

| Method | When to use | Memory guardrails |
|--------|-------------|-------------------|
| **A. `run.sh` (local/dev)** | Testing on your own machine | JVM heap cap (`-Xmx256m`) |
| **B. systemd service (recommended for prod)** | The admin runs it as a long-lived service | JVM heap cap **+** kernel hard cap (`MemoryMax=512M`, `MemorySwapMax=0`) |
| **C. `run.sh` on a server (no systemd)** | A server that doesn't use systemd | JVM heap cap only |

> **Never run `java -jar target/log-tailer.jar` directly on a shared server.**
> A bare launch lets the JVM grow its heap to 25% of the machine's RAM and uses
> a GC that does not return memory to the OS, so over time it can slow down the
> whole host. Every method below applies `-Xmx256m -XX:+UseG1GC` to prevent this.

### Method A — `run.sh` (local / dev)

```bash
./run.sh --config=/path/to/config.json
```

By default `run.sh` launches `target/log-tailer.jar` (where `mvn package` puts
it). If the jar lives elsewhere, point at it with `JAR=` — no need to edit the
script:

```bash
JAR=/opt/log-tailer/log-tailer.jar ./run.sh --config=/path/to/config.json
```

Override the heap if needed:

```bash
JVM_MAX_HEAP=512m ./run.sh --config=/path/to/config.json
```

### Method B — systemd service (recommended for production)

See [Production deployment (systemd)](#production-deployment-systemd) below. This
is the right choice when the admin runs the tool **as a service**: it starts on
boot, restarts on crash, and enforces a hard kernel memory cap. The JVM flags are
baked into the unit's `ExecStart`, so it can never be launched bare by accident.

### Method C — `run.sh` on a server without systemd

If a server doesn't use systemd, copy the jar, the config, **and `run.sh`** to the
box and launch it (e.g. under `nohup` or a process manager):

```bash
nohup env JAR=/opt/log-tailer/log-tailer.jar \
  ./run.sh --config=/opt/log-tailer/config.json >/var/log/log-tailer.out 2>&1 &
```

> Note: this gives you the `-Xmx` heap cap but **not** the kernel-level
> `MemoryMax`/`MemorySwapMax` hard wall, and nothing restarts it on crash. Prefer
> Method B whenever systemd is available.

### Config path resolution (in priority order)

1. CLI argument: `--config=/path/to/config.json`
2. Environment variable: `LOGTAILER_CONFIG=/path/to/config.json`
3. JVM property: `-Dlogtailer.config=/path/to/config.json`
4. Default: `config/logTailer_config.json` (external file first, then bundled classpath resource)

---

## Production deployment (systemd)

For a real server, run it under systemd so it restarts on failure and the kernel
enforces a hard memory limit — a second wall behind the JVM's `-Xmx`. A unit file
is provided at [`deploy/log-tailer.service`](deploy/log-tailer.service).

**No repo clone needed on the server.** Copy just three files from the build
machine: the jar, your real config, and `log-tailer.service`. `run.sh` is **not**
used here — the JVM flags live inside the unit's `ExecStart`. Adjust the two paths
and the `User=` in the unit if your layout differs.

```bash
# 1. Create a dedicated unprivileged user that can READ the log files and reach Kafka
sudo useradd --system --no-create-home logtailer
sudo adduser logtailer adm        # 'adm' usually owns /var/log

# 2. Place the artifact and the REAL config (not the repo's empty template)
sudo install -D target/log-tailer.jar /opt/log-tailer/log-tailer.jar
sudo install -D -m 600 your-config.json /etc/log-tailer/config.json
sudo chown logtailer:logtailer /etc/log-tailer/config.json

# 3. Install and start the service
sudo cp deploy/log-tailer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now log-tailer
```

The unit caps resources so a misbehaving process can never starve the host:
`MemoryMax=512M` + `MemorySwapMax=0` (kernel kills + restarts instead of swapping
the box), `CPUQuota=50%`, and `Nice=10` (yields CPU to other apps). It also runs
with filesystem hardening (`ProtectSystem=strict`, `ProtectHome=read-only`,
`NoNewPrivileges`) since the tool only ever reads.

> **Check the Java path first.** The unit's `ExecStart` uses `/usr/bin/java`,
> which is correct for a system-installed JDK. Run `which java` on the server; if
> it prints a different path, edit that line in the unit to match.

Watch it after deploy:

```bash
journalctl -u log-tailer -f          # logs / errors
systemctl status log-tailer          # current memory usage vs. the limit
```

### Verifying the memory limit

The `MemoryMax=512M` + `MemorySwapMax=0` settings are only enforced if the host's
cgroup **memory controller** is active. It is by default on modern Linux
(Ubuntu 22.04+, Debian 11+, RHEL/Rocky/Alma 9+, Fedora 31+). Confirm after deploy:

```bash
# 1. The kernel must list the memory controller
cat /sys/fs/cgroup/cgroup.controllers          # output must contain: memory

# 2. systemd must report the limits on the unit
systemctl show log-tailer -p MemoryMax -p MemorySwapMax
# expect: MemoryMax=536870912   MemorySwapMax=0   (NOT "infinity")

# 3. The cgroup file must show the real number, not "max"
cat /sys/fs/cgroup/system.slice/log-tailer.service/memory.max   # expect: 536870912
```

To prove the cap actually kills an over-budget process **without touching prod**,
run a deliberate memory eater under the same limits on any machine:

```bash
systemd-run --scope -p MemoryMax=512M -p MemorySwapMax=0 \
  python3 -c 'import time
c=[]
mb=0
while True:
    c.append(bytearray(20*1024*1024)); mb+=20
    print(mb,"MB",flush=True); time.sleep(0.05)'
```

It climbs to ~500 MB and is OOM-killed; the journal records `512M memory peak`
and `result oom-kill`. That confirms the process can never exceed 512 MB of RAM
and cannot spill into swap. (Use `sudo` for a system-wide scope, or `--user` if
the memory controller is delegated to your user manager.)

---

## Configuration

The application reads a single JSON file.

### Full example — `config.json`

```json
{
  "bootstrapServers": "192.168.60.135:9092",

  "identity": {
    "system": {
      "id": "system-E",
      "name": "My System Name"
    },
    "server": {
      "name": "linux-mint-vm",
      "ip": "192.168.60.11"
    }
  },

  "logTailer": {
    "enabled": true,
    "files": [
      { "path": "/var/log/app/app.log",    "topic": "app-logs" },
      { "path": "/var/log/app/system.log", "topic": "system-logs" }
    ]
  },

  "storageMonitoring": {
    "enabled": true,
    "topic": "server-storage-snapshot",
    "paths": [
      "/",
      "/mnt/data"
    ],
    "intervalHours": 12
  }
}
```

### Fields reference

#### `bootstrapServers`
Kafka broker address(es). Used by all producers.

#### `identity`
Metadata used in published Kafka messages. Log events include `identity.server.name`; storage snapshots include the configured system and server identity fields.

| Field | Description |
|-------|-------------|
| `identity.system.id` | Logical system identifier |
| `identity.system.name` | Human-readable system name (Unicode supported) |
| `identity.server.name` | Server hostname |
| `identity.server.ip` | Server IP address |

#### `logTailer`

| Field | Description |
|-------|-------------|
| `enabled` | Enable or disable log tailing |
| `files[].path` | Absolute path to the log file |
| `files[].topic` | Kafka topic to publish log events to |

- One thread is spawned per configured file
- If a log file does not exist yet, the tailer waits for it to appear
- Initial startup begins at the end of an existing file; recreated, rotated, or truncated files are read from the beginning

#### `storageMonitoring`

| Field | Description |
|-------|-------------|
| `enabled` | Enable or disable storage monitoring |
| `topic` | Kafka topic to publish snapshots to |
| `paths` | List of directories or mount points to measure |
| `intervalHours` | How often to collect and publish a snapshot |

---

## Kafka message formats

### Log event (logTailer)

```json
{
  "serverName": "linux-mint-vm",
  "path": "/var/log/app/app.log",
  "topic": "app-logs",
  "timestamp": "2024-01-15T10:30:00Z",
  "message": "INFO  Application started"
}
```

### Storage snapshot (storageMonitoring)

```json
{
  "systemId": "system-E",
  "systemName": "My System Name",
  "serverName": "linux-mint-vm",
  "serverIp": "192.168.60.11",
  "timestamp": "2024-01-15T10:30:00Z",
  "diskUsages": [
    {
      "path": "/",
      "totalBytes": 107374182400,
      "usedBytes": 32212254720,
      "usedPercent": 30.0
    }
  ]
}
```

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `LOGTAILER_CONFIG` | Path to config JSON (alternative to CLI arg) |
| `HOST_FS` | Filesystem prefix for containerized environments (e.g. `/host`). When set, storage paths are resolved as `HOST_FS + path` so the host filesystem can be measured from inside a container. |

---

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| `kafka-clients` | 3.7.1 | Kafka producer |
| `jackson-databind` | 2.15.4 | JSON serialization |
| `slf4j-simple` | 1.7.36 | Logging |

---

## License

Internal / Proprietary — specify your license here.
