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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class LogTailer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(LogTailer.class);
    private static final int MAX_LINE_LENGTH = 1024 * 1024; // 1 MB
    private static final int READ_BUFFER_SIZE = 64 * 1024;  // 64 KB

    // Rotation/existence checks each do a stat() syscall; running them every poll wastes
    // I/O on the hot path. Run them roughly every 5s (25 * 200ms) instead.
    private static final int ROTATION_CHECK_CYCLES = 25;
    private static final long POLL_INTERVAL_MS = 200;
    private static final long ERROR_LOG_INTERVAL_MS = 5000;

    private final Path filePath;
    private final String filePathStr;
    private final String topic;
    private final String serverName;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    private long filePointer = 0;
    private Object fileKey;
    private final StringBuilder lineBuffer = new StringBuilder();
    private CharsetDecoder decoder = newDecoder();

    // Reused once-allocated read buffers (no per-read garbage). byteBuffer is kept in fill
    // mode and compacted after each decode so a multibyte char split across a read boundary
    // is carried into the next read instead of being dropped.
    private final byte[] rawBytes = new byte[READ_BUFFER_SIZE];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap(rawBytes);
    private final CharBuffer charBuffer = CharBuffer.allocate(READ_BUFFER_SIZE);

    // Throttled delivery-failure logging (callbacks run on the single producer network thread).
    private long deliveryFailures = 0;
    private long lastErrorLogMs = 0;

    public LogTailer(
            Path filePath,
            String topic,
            String serverName,
            KafkaProducer<String, String> producer
    ) {
        this.filePath = filePath;
        this.filePathStr = filePath.toString();
        this.topic = topic;
        this.serverName = serverName;
        this.producer = producer;
    }

    @Override
    public void run() {
        log.info("Starting log tailer: {} -> topic: {}", filePath, topic);

        RandomAccessFile raf = null;
        boolean startAtEnd = true; // only the very first open skips existing content
        int cycle = 0;

        try {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (raf == null) {
                        if (!Files.exists(filePath)) {
                            log.warn("Waiting for log file to appear: {}", filePath);
                            Thread.sleep(1000);
                            continue;
                        }
                        raf = openFile(startAtEnd);
                        startAtEnd = false;
                    }

                    // Periodic rotation/disappearance detection (off the hot path).
                    if (++cycle % ROTATION_CHECK_CYCLES == 0) {
                        if (!Files.exists(filePath)) {
                            log.warn("Log file disappeared: {}", filePath);
                            closeQuietly(raf);
                            raf = null;
                            clearBuffer();
                            continue;
                        }
                        if (hasBeenReplaced()) {
                            log.info("Log file replaced or rotated, reopening: {}", filePath);
                            closeQuietly(raf);
                            raf = null;
                            clearBuffer();
                            continue;
                        }
                    }

                    long length = raf.length();

                    if (length < filePointer) {
                        log.info("Log file truncated, reopening: {}", filePath);
                        closeQuietly(raf);
                        raf = null;
                        clearBuffer();
                        continue;
                    }

                    if (length > filePointer) {
                        raf.seek(filePointer);
                        readNewBytes(raf);
                        filePointer = raf.getFilePointer();
                    }

                    Thread.sleep(POLL_INTERVAL_MS);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;

                } catch (Exception e) {
                    // Self-heal: log and retry instead of letting the thread die permanently.
                    log.error("Recoverable error in log tailer for {}, retrying", filePath, e);
                    closeQuietly(raf);
                    raf = null;
                    clearBuffer();
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        } finally {
            closeQuietly(raf);
        }
    }

    /**
     * Initial startup skips existing data; reopened files are read from their beginning.
     */
    private RandomAccessFile openFile(boolean startAtEnd) throws IOException {
        Object openedFileKey = readFileKey();
        RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "r");
        fileKey = openedFileKey;
        filePointer = startAtEnd ? raf.length() : 0;
        return raf;
    }

    private boolean hasBeenReplaced() throws IOException {
        Object currentFileKey;
        try {
            currentFileKey = readFileKey();
        } catch (NoSuchFileException e) {
            return false;
        }
        return fileKey != null && currentFileKey != null && !fileKey.equals(currentFileKey);
    }

    private Object readFileKey() throws IOException {
        return Files.readAttributes(filePath, BasicFileAttributes.class).fileKey();
    }

    /**
     * Clears the line buffer, the byte carry-over buffer, and resets the decoder state.
     */
    private void clearBuffer() {
        lineBuffer.setLength(0);
        byteBuffer.clear();
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
     * Streams bytes through a stateful CharsetDecoder. The byteBuffer is filled, decoded,
     * then compacted so any trailing bytes of a multibyte UTF-8 char that was split across
     * the read boundary are preserved for the next read instead of being lost.
     */
    private void readNewBytes(RandomAccessFile raf) throws IOException {
        int read;
        while (byteBuffer.hasRemaining()
                && (read = raf.read(rawBytes, byteBuffer.position(), byteBuffer.remaining())) > 0) {

            byteBuffer.position(byteBuffer.position() + read);
            byteBuffer.flip();

            charBuffer.clear();
            decoder.decode(byteBuffer, charBuffer, false);
            charBuffer.flip();
            lineBuffer.append(charBuffer);

            byteBuffer.compact(); // keep undecoded trailing bytes for the next read
            flushCompleteLines();
        }
    }

    /**
     * Sends only complete lines to Kafka. Uses a single compaction of the line buffer after
     * extracting all complete lines, avoiding O(n) shifts per line. Guards against unbounded
     * lineBuffer growth when no newline is encountered.
     */
    private void flushCompleteLines() {

        if (lineBuffer.length() > MAX_LINE_LENGTH) {
            log.warn("Line exceeds {} bytes in {}, flushing oversized chunk", MAX_LINE_LENGTH, filePath);
            String oversized = lineBuffer.substring(0, MAX_LINE_LENGTH);
            lineBuffer.setLength(0);
            sendToKafka(oversized);
            return;
        }

        int readOffset = 0;
        int index;
        while ((index = lineBuffer.indexOf("\n", readOffset)) >= 0) {
            String line = lineBuffer.substring(readOffset, index);
            readOffset = index + 1;

            // Strip BOM and \r only — trim() is NOT used because it removes
            // intentional leading/trailing whitespace (e.g. indented stack traces)
            String msg = line
                    .replace("\uFEFF", "")
                    .replace("\r", "");

            if (msg.isEmpty()) {
                continue;
            }

            sendToKafka(msg);
        }

        if (readOffset > 0) {
            lineBuffer.delete(0, readOffset);
        }
    }

    private void sendToKafka(String msg) {
        try {
            LogEvent event = new LogEvent(
                    serverName,
                    filePathStr,
                    topic,
                    DateTimeFormatter.ISO_INSTANT.format(Instant.now()),
                    msg
            );

            String payload = mapper.writeValueAsString(event);

            producer.send(
                    new ProducerRecord<>(this.topic, this.serverName, payload),
                    // Throttled failure logging: at most one aggregated error per interval, so a
                    // broker outage cannot flood synchronous stderr and worsen the load.
                    (metadata, ex) -> {
                        if (ex != null) {
                            this.deliveryFailures++;
                            long now = System.currentTimeMillis();
                            if (now - this.lastErrorLogMs >= ERROR_LOG_INTERVAL_MS) {
                                log.error("Kafka delivery failing | topic={} file={} failuresSinceLastLog={} lastError={}",
                                        topic, filePath, this.deliveryFailures, ex.getMessage());
                                this.lastErrorLogMs = now;
                                this.deliveryFailures = 0;
                            }
                        }
                    }
            );

        } catch (Exception e) {
            log.error("Failed to serialize log event from: {}", filePath, e);
        }
    }
}
