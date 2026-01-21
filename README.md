
---

# ⚙️ Kafka File Log Producer

A lightweight Java-based log ingestion service that **monitors local log files** in real time and streams every new line directly into **Kafka topics**.
Each file is mapped to a Kafka topic (e.g., `app1.log` → `app1-topic`), enabling flexible and scalable log forwarding.

---

## 🧩 How It Works

1. The application reads **`config.json`**, which defines:

   * Kafka bootstrap servers
   * Files to watch
   * Target Kafka topics for each file

2. Before starting, the app uses **`KafkaTopicValidator`** to confirm all Kafka topics exist.

3. A **dedicated thread** is launched for each file.

4. Each thread continuously:

   * Watches the file for newly appended lines
   * Sends each new line to the specified Kafka topic

5. The program runs indefinitely until manually stopped.

---

## 📁 Example `config.json`

```json
{
  "bootstrapServers": "192.168.60.135:9092",
  "files": [
    { "path": "/home/kafkaproducer/log_file/app1.log", "topic": "app1-topic" },
    { "path": "/home/kafkaproducer/log_file/app2.log", "topic": "app2-topic" },
    { "path": "/home/kafkaproducer/log_file/system.log", "topic": "system-topic" }
  ]
}
```

### ➕ Add More Files

Just add a new entry:

```json
{ "path": "/home/kafkaproducer/log_file/newapp.log", "topic": "newapp-topic" }
```

Restart the app and it will automatically begin monitoring the new file.

---

## 🧰 Requirements

* **Java 17+**
* **Apache Kafka 4.x** running on your server
* Kafka topics created (validated at startup)
* Read permissions on the watched log files

---

## ▶️ Running the Application

### 1. Build the JAR

```bash
mvn clean package
```

### 2. Run the JAR

```bash
java -jar target/KafkaProducerApp-1.0.jar
```

### Example Console Output

```
[21:02:42] Validated topic exists: app1-topic
[21:02:42] Validated topic exists: app2-topic
[21:02:45] Watching file: /home/kafkaproducer/log_file/app1.log -> Topic: app1-topic
[21:02:50] Topic: app1-topic Sent message: INFO Application started
```

Press **Ctrl + C** to stop gracefully.

---

## 🧠 Project Structure

```
src/
 └── main/
     ├── java/
     │   └── org/munycha/kafkaproducer/
     │       ├── AppMain.java                       # Program entry point
     │       ├── config/
     │       │   ├── ConfigLoader.java              # Reads and parses config.json
     │       │   ├── ConfigData.java                # Represents full config
     │       │   └── FileItem.java                  # One file/topic mapping
     │       ├── producer/
     │       │   ├── FileWatcher.java               # Watches file + sends logs to Kafka
     │       │   └── KafkaFactory.java              # Creates KafkaProducer instance
     │       └── utility/
     │           └── KafkaTopicValidator.java       # Validates Kafka topics before startup
     └── resources/
         ├── config.json                            # Main application configuration
         └── simplelogger.properties                # Controls SLF4J logging levels

```

---

## 🧩 Class Overview

| Class                   | Purpose                                                                |
|-------------------------| ---------------------------------------------------------------------- |
| **AppMain**             | Loads configuration, validates topics, and starts all watcher threads  |
| **FileWatcher**         | Streams new log lines to Kafka (tail-like behavior)                    |
| **KafkaFactory**        | Creates and configures Kafka producers                                 |
| **ConfigLoader**        | Reads `config.json` using Jackson                                      |
| **ConfigData**          | Represents full configuration (bootstrap + file list)                  |
| **FileItem**            | Represents a single file → topic assignment                            |
| **KafkaTopicValidator** | Checks if Kafka topics exist before startup; prevents misconfiguration |

---

## 💡 Tips & Best Practices

* Keep `config.json` inside `src/main/resources` for packaging convenience.
* Ensure log files **exist before starting** the producer.
* Use this producer with your **Kafka File Log Consumer** to build a full E2E log pipeline.
* If you change topics or file paths, update `config.json` and restart the app.

---

## 🧑‍💻 Author

**Munycha**
Kafka Learning Project — Java + Apache Kafka (Real-Time File Watcher Producer)

---


