package org.munycha.logtailer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.munycha.logtailer.config.AppConfig;
import org.munycha.logtailer.config.ConfigLoader;
import org.munycha.logtailer.config.ConfigPathResolver;
import org.munycha.logtailer.config.LogFileConfig;
import org.munycha.logtailer.service.LogTailer;
import org.munycha.logtailer.service.KafkaProducerFactory;
import org.munycha.logtailer.service.StorageMonitorTask;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LogTailerApplication {

    public static void main(String[] args) throws Exception {

        // Load config ONCE
        String configPath = ConfigPathResolver.resolve(
                args,
                "LOGTAILER_CONFIG",          // ENV var
                "logtailer.config",                 // JVM system property
                "config/logTailer_config.json"      // classpath default
        );

        System.out.println("[Config] Using config path: " + configPath);

        ConfigLoader loader = new ConfigLoader(configPath);
        AppConfig config = loader.load();

        // Kafka producer
        KafkaProducerFactory factory =
                new KafkaProducerFactory(config.getBootstrapServers());
        KafkaProducer<String, String> producer = factory.createProducer();


        //File watcher thread
        ExecutorService logTailerExecutor = null;

        if (config.getLogTailer() != null && config.getLogTailer().isEnabled()) {

            List<LogFileConfig> files = config.getLogTailer().getFiles();

            if (files == null || files.isEmpty()) {
                throw new IllegalStateException("LogTailer enabled but no files configured");
            }

            logTailerExecutor = Executors.newFixedThreadPool(files.size());

            for (LogFileConfig f : files) {
                logTailerExecutor.submit(
                        new LogTailer(
                                Paths.get(f.getPath()),
                                f.getTopic(),
                                config.getIdentity().getServer().getName(),
                                producer
                        )
                );
            }
        }


        //Storage monitor thread
        ScheduledExecutorService storageScheduler = null;

        if (config.getStorageMonitoring() != null &&
                config.getStorageMonitoring().isEnabled()) {

            storageScheduler = Executors.newSingleThreadScheduledExecutor();

            storageScheduler.scheduleAtFixedRate(
                    new StorageMonitorTask(producer, config),
                    0,
                    config.getStorageMonitoring().getIntervalHours(),
                    TimeUnit.HOURS
            );
        }


        //shutdown
        ExecutorService finalExecutorService = logTailerExecutor;
        ScheduledExecutorService finalStorageScheduler = storageScheduler;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            // Signal all worker threads to stop
            if (finalStorageScheduler != null) finalStorageScheduler.shutdownNow();
            if (finalExecutorService != null) finalExecutorService.shutdownNow();

            // Wait for threads to finish before touching the shared producer
            try {
                if (finalExecutorService != null)
                    finalExecutorService.awaitTermination(10, TimeUnit.SECONDS);
                if (finalStorageScheduler != null)
                    finalStorageScheduler.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            // Safe to flush and close now — no worker thread is sending anymore
            try {
                producer.flush();
            } catch (Exception ignored) {}

            producer.close();
        }));

        new java.util.concurrent.CountDownLatch(1).await();

    }
}
