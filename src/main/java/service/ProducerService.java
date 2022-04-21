package service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class ProducerService {
    private final String topicName;
    private final Properties properties;

    public ProducerService(String bootstrapServers, String topicName) {
        this.topicName = topicName;
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    public void sendRecords(List<String> records) {
        Producer<String, String> producer = new KafkaProducer<>(properties);
        records.forEach(e -> producer.send(new ProducerRecord<>(topicName, e)));
    }
}
