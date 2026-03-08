# log-tailer

**log-tailer** is a lightweight, **Java 8-compatible** application that tails log files and collects server storage metrics, publishing everything to **Apache Kafka**.

It provides:
- Continuous tailing of multiple log files → Kafka topics
- Periodic disk usage snapshots → Kafka topic
- System and server identity attached to every message

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

```bash
java -jar target/log-tailer.jar --config=/path/to/config.json
```

### Config path resolution (in priority order)

1. CLI argument: `--config=/path/to/config.json`
2. Environment variable: `LOG_TAILER_CONFIG=/path/to/config.json`
3. JVM property: `-Dlog.tailer.config=/path/to/config.json`
4. Default: `config/config.json` (relative to working directory)

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
Metadata stamped on every Kafka message.

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
- File rotation and truncation are handled automatically

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
| `LOG_TAILER_CONFIG` | Path to config JSON (alternative to CLI arg) |
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
