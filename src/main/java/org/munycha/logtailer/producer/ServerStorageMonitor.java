package org.munycha.logtailer.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.munycha.logtailer.config.AppConfig;
import org.munycha.logtailer.model.MountPathStorageUsage;
import org.munycha.logtailer.model.ServerStorageSnapshot;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ServerStorageMonitor implements Runnable {
    private final KafkaProducer<String, String> producer;
    private final AppConfig config;

    private final ObjectMapper mapper = new ObjectMapper();

    public ServerStorageMonitor(KafkaProducer<String,String> producer, AppConfig config){
        this.producer = producer;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            List<MountPathStorageUsage> mountPathStorageUsages = MountPathStorageUsageCollector.collect(this.config.getStorageMonitoring().getPaths());

            ServerStorageSnapshot serverStorageSnapshot = new ServerStorageSnapshot();
            serverStorageSnapshot.setSystemId(config.getIdentity().getSystem().getId());
            serverStorageSnapshot.setSystemName(config.getIdentity().getSystem().getName());
            serverStorageSnapshot.setServerName(config.getIdentity().getServer().getName());
            serverStorageSnapshot.setServerIp(config.getIdentity().getServer().getIp());

            String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

            serverStorageSnapshot.setTimestamp(timestamp);
            serverStorageSnapshot.setMountPathStorageUsages(mountPathStorageUsages);

            String json = mapper.writeValueAsString(serverStorageSnapshot);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            config.getStorageMonitoring().getTopic(),
                            serverStorageSnapshot.getServerName(),
                            json
                    );

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println(
                            "FAILED TO SEND SERVER STORAGE SNAPSHOT | "
                                    + "server=" + serverStorageSnapshot.getServerName()
                                    + " | topic=" + record.topic()
                    );
                    exception.printStackTrace();
                }
            });


            producer.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

