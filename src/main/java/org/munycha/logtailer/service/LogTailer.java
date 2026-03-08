package org.munycha.logtailer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.munycha.logtailer.model.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class LogTailer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LogTailer.class);
    private static final int MAX_LINE_LENGTH = 1024 * 1024; // 1 MB
    private static final int READ_BUFFER_SIZE = 4096;

    private final Path filePath;
    private final String topic;
    private final String serverName;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    private long filePointer = 0;
    private final StringBuilder lineBuffer = new StringBuilder();
    private CharsetDecoder decoder = newDecoder();

    public LogTailer(
            Path filePath,
            String topic,
            String serverName,
            KafkaProducer<String, String> producer
    ) {
        this.filePath = filePath;
        this.topic = topic;
        this.serverName = serverName;
        this.producer = producer;
    }

    @Override
    public void run() {
        log.info("Starting log tailer: {} -> topic: {}", filePath, topic);

        RandomAccessFile raf = null;

        try {
            // Fix 3: Wait for file to appear instead of permanently exiting the thread
            while (!Files.exists(filePath)) {
                if (Thread.currentThread().isInterrupted()) return;
                log.warn("Waiting for log file to appear: {}", filePath);
                Thread.sleep(1000);
            }

            raf = openAtEnd();

            while (!Thread.currentThread().isInterrupted()) {

                // Fix 1: File disappeared — close raf and wait for reappearance,
                // then reopen a fresh handle pointing to the new file inode
                if (!Files.exists(filePath)) {
                    log.warn("Log file disappeared: {}", filePath);
                    closeQuietly(raf);
                    raf = null;
                    clearBuffer();

                    while (!Files.exists(filePath)) {
                        if (Thread.currentThread().isInterrupted()) return;
                        Thread.sleep(500);
                    }

                    log.info("Log file reappeared, reopening: {}", filePath);
                    raf = openAtEnd();
                    continue;
                }

                long length = raf.length();

                // File truncated or rotated — reopen from position 0 to capture new content
                if (length < filePointer) {
                    log.info("Log file truncated or rotated: {}", filePath);
                    closeQuietly(raf);
                    clearBuffer();
                    raf = new RandomAccessFile(filePath.toFile(), "r");
                    filePointer = 0;
                }

                // New data available
                if (length > filePointer) {
                    raf.seek(filePointer);
                    readNewBytes(raf);
                    filePointer = raf.getFilePointer();
                }

                Thread.sleep(200);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

        } catch (Exception e) {
            log.error("Fatal error in log tailer for: {}", filePath, e);

        } finally {
            closeQuietly(raf);
        }
    }

    /**
     * Opens the file and positions at the end (tail -f behaviour).
     */
    private RandomAccessFile openAtEnd() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
        filePointer = raf.length();
        return raf;
    }

    /**
     * Clears the line buffer and resets the UTF-8 decoder state for a fresh file.
     */
    private void clearBuffer() {
        lineBuffer.setLength(0);
        decoder = newDecoder();
    }

    private static CharsetDecoder newDecoder() {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    private static void closeQuietly(RandomAccessFile raf) {
        if (raf != null) {
            try { raf.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Fix 2: Uses a stateful streaming CharsetDecoder so multi-byte UTF-8 characters
     * split across read-buffer boundaries are decoded correctly without corruption.
     */
    private void readNewBytes(RandomAccessFile raf) throws IOException {
        byte[] rawBytes = new byte[READ_BUFFER_SIZE];
        CharBuffer charBuffer = CharBuffer.allocate(READ_BUFFER_SIZE * 2);
        int read;

        while ((read = raf.read(rawBytes)) > 0) {
            ByteBuffer in = ByteBuffer.wrap(rawBytes, 0, read);
            charBuffer.clear();
            decoder.decode(in, charBuffer, false);
            charBuffer.flip();
            lineBuffer.append(charBuffer);
            flushCompleteLines();
        }
    }

    /**
     * Fix 4: Sends only complete lines to Kafka.
     * Guards against unbounded lineBuffer growth when no newline is encountered.
     */
    private void flushCompleteLines() {

        if (lineBuffer.length() > MAX_LINE_LENGTH) {
            log.warn("Line exceeds {} bytes in {}, flushing oversized chunk", MAX_LINE_LENGTH, filePath);
            String oversized = lineBuffer.substring(0, MAX_LINE_LENGTH);
            lineBuffer.setLength(0);
            sendToKafka(oversized);
            return;
        }

        int index;
        while ((index = lineBuffer.indexOf("\n")) >= 0) {
            String line = lineBuffer.substring(0, index);
            lineBuffer.delete(0, index + 1);

            // Fix 8: Strip BOM and \r only — trim() is NOT used because it removes
            // intentional leading/trailing whitespace (e.g. indented stack traces)
            String msg = line
                    .replace("\uFEFF", "")
                    .replace("\r", "");

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
                    new ProducerRecord<>(this.topic, this.serverName, payload),
                    // Fix 5 & 7: Log Kafka delivery failures via SLF4J with full context
                    (metadata, ex) -> {
                        if (ex != null) {
                            log.error("Failed to deliver to Kafka | topic={} file={} error={}",
                                    topic, filePath, ex.getMessage());
                        }
                    }
            );

        } catch (Exception e) {
            log.error("Failed to serialize log event from: {}", filePath, e);
        }
    }
}
