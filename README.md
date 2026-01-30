
````md
# log-tailer

**log-tailer** is a **Java 8–compatible log tailing and system monitoring application** that publishes data to **Apache Kafka**.

It provides:
- Continuous tailing of multiple log files and publishing entries to Kafka topics
- Periodic server storage usage snapshots
- Consistent system and server identity attached to every message

---

## Requirements

- **Java:** 1.8 (Java 8)
- **Build tool:** Maven 3.6+
- **Kafka:** Reachable Kafka broker
- **OS:** Linux (paths and log tailing are Linux-oriented)

Verify Java:
```bash
java -version
````

---

## Build

```bash
mvn clean package
```

### Output

```text
target/log-tailer.jar
```

---

## Run

```bash
java -jar /path/to/log-tailer.jar --config=/path/to/config/config.json
```

### Example

```bash
java -jar target/log-tailer.jar --config=./config/config.json
```

> The application requires an external JSON configuration file.

---

## Configuration Overview

The application is configured using **one JSON file**.

### Example: `config.json`

```json
{
  "bootstrapServers": "192.168.60.135:9092",

  "identity": {
    "system": {
      "id": "system-E",
      "name": "ប្រព័ន្ធគ្រប់គ្រងទិន្នន័យប្រាក់ខែបុគ្គលិក"
    },
    "server": {
      "name": "linux-mint-vm",
      "ip": "192.168.60.11"
    }
  },

  "logTailer": {
    "enabled": true,
    "files": [
      { "path": "/data/input/logs/app1.log", "topic": "app1-topic" },
      { "path": "/data/input/logs/app2.log", "topic": "app2-topic" },
      { "path": "/data/input/logs/app3.log", "topic": "app3-topic" },
      { "path": "/data/input/logs/app4.log", "topic": "app4-topic" },
      { "path": "/data/input/logs/system.log", "topic": "system-topic" },
      { "path": "/data/input/logs/server.log", "topic": "server-topic" }
    ]
  },

  "storageMonitoring": {
    "enabled": true,
    "topic": "server-storage-snapshot",
    "paths": [
      "/",
      "/mnt/data-pressure",
      "/mnt/medium-usage",
      "/mnt/high-usage",
      "/run"
    ],
    "intervalHours": 12
  }
}
```

---

## Configuration Details

### Kafka Connection

```json
"bootstrapServers": "192.168.60.135:9092"
```

* Kafka bootstrap server list
* Used by all Kafka producers in the application

---

### Identity

Metadata attached to every Kafka message.

* **System**

   * `id`: Logical system identifier
   * `name`: Human-readable system name (Unicode supported)

* **Server**

   * `name`: Server hostname
   * `ip`: Server IP address

---

### Log Tailer

When enabled:

* Each configured file is tailed continuously
* New log entries are published to the assigned Kafka topic

Fields:

* `path`: Absolute path to the log file
* `topic`: Kafka topic receiving log events

---

### Storage Monitoring

When enabled:

* Disk usage snapshots are collected periodically
* Snapshots are published to Kafka

Fields:

* `topic`: Kafka topic for storage metrics
* `paths`: Directories or mount points to monitor
* `intervalHours`: Snapshot interval in hours

---

## Kafka Topics Used

| Purpose           | Topic                            |
| ----------------- | -------------------------------- |
| Application logs  | `app1-topic`, `app2-topic`, etc. |
| System logs       | `system-topic`, `server-topic`   |
| Storage snapshots | `server-storage-snapshot`        |

---

## Java Compatibility

* Compiled with **Java 8**
* Bytecode target: **Java 8**
* Runs on Java 8 runtime without additional flags

---

## Notes

* Log files must exist and be readable
* Kafka topics must exist or auto-creation must be enabled
* All paths are evaluated on the local server only

---

## Typical Use Cases

* Centralized log aggregation
* Infrastructure and disk monitoring
* Feeding logs and metrics into Kafka pipelines
* Lightweight alternative to full log agents

---

## License

Specify your license here (e.g. Internal, Proprietary, Apache 2.0).

---

## Maintainer

* **Project:** log-tailer
* **Runtime:** Java 8
* **Build:** Maven

```
