package org.munycha.logTailer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.munycha.logTailer.model.LogEvent;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class LogTailer implements Runnable {

    private final Path filePath;
    private final String topic;
    private final String serverName;
    private final KafkaProducer<String, String> producer;
    private final Properties producerProps;
    private final ObjectMapper mapper = new ObjectMapper();

    private long filePointer = 0;
    private final StringBuilder lineBuffer = new StringBuilder();

    public LogTailer(
            Path filePath,
            String topic,
            String serverName,
            KafkaProducer<String, String> producer,
            Properties producerProps
    ) {
        this.filePath = filePath;
        this.topic = topic;
        this.serverName = serverName;
        this.producer = producer;
        this.producerProps = producerProps;
    }

    @Override
    public void run() {

        if (!Files.exists(filePath)) {
            System.err.println("Log file does NOT exist: " + filePath);
            return;
        }

        System.out.printf("Tailing file (polling): %s -> Topic: %s%n", filePath, topic);

        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(filePath.toFile(), "r");

            // Start at end (tail -f behavior)
            filePointer = raf.length();

            while (!Thread.currentThread().isInterrupted()) {

                // File disappeared (rotation or delete)
                if (!Files.exists(filePath)) {
                    filePointer = 0;
                    lineBuffer.setLength(0);

                    // Wait until file reappears
                    Thread.sleep(500);
                    continue;
                }

                long length = raf.length();

                // File truncated or rotated
                if (length < filePointer) {
                    raf.close();
                    raf = new RandomAccessFile(filePath.toFile(), "r");
                    filePointer = 0;
                    lineBuffer.setLength(0);
                }

                // New data available
                if (length > filePointer) {
                    raf.seek(filePointer);
                    readNewBytes(raf);
                    filePointer = raf.getFilePointer();
                }

                // Poll interval (like tail -f)
                Thread.sleep(200);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            try {
                if (raf != null) raf.close();
            } catch (Exception ignored) {
            }
        }
    }


    /**
     * Reads bytes safely, decodes UTF-8, buffers partial lines.
     */
    private void readNewBytes(RandomAccessFile raf) throws IOException {

        byte[] buffer = new byte[4096];
        int read;

        while ((read = raf.read(buffer)) > 0) {
            String chunk = new String(buffer, 0, read, StandardCharsets.UTF_8);
            lineBuffer.append(chunk);
            flushCompleteLines();
        }
    }

    /**
     * Sends only complete lines to Kafka.
     */
    private void flushCompleteLines() {

        int index;
        while ((index = lineBuffer.indexOf("\n")) >= 0) {

            String line = lineBuffer.substring(0, index);
            lineBuffer.delete(0, index + 1);

            String msg = line
                    .replace("\uFEFF", "")
                    .replace("\r", "")
                    .trim();

            if (msg.isEmpty()) {
                continue;
            }

            sendToKafka(msg);
        }
    }


    private void sendToKafka(String msg) {
        try {
            LogEvent event = new LogEvent(
                    serverName,
                    filePath.toString(),
                    topic,
                    DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
                    msg
            );

            String payload = mapper.writeValueAsString(event);

            producer.send(
                    new ProducerRecord<>(this.topic,this.serverName, payload),
                    (metadata, ex) -> {
                        if (ex != null) {
                            System.err.printf(
                                    "[%s] Topic: %s Error: %s%n",
                                    java.time.LocalTime.now(),
                                    topic,
                                    ex.getMessage()
                            );
                        }else{
                            System.out.printf("[%s] Topic: %-15s Sent log: %s%n",java.time.LocalDateTime.now(), topic, msg);
                        }
                    }
            );

        } catch (Exception e) {
            System.err.println("Failed to serialize/send log event: " + e.getMessage());
        }
    }
}

