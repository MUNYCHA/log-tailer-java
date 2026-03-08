package org.munycha.logtailer.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerFactory {

    private final Properties producerProps;

    public KafkaProducerFactory(String bootstrapServers) {

        // Initialize producer configuration
        producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Serialize keys and values as Strings
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Fast message acknowledgment
        producerProps.put(ProducerConfig.ACKS_CONFIG, "1");

        // Send immediately with no artificial delay
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
    }


    public KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(producerProps);
    }

    public Properties getProducerProps() {
        return producerProps;
    }
}
