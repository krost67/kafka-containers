package service;

import kafka.ReusableKafkaContainer;
import kafka.ReuseKafkaContainerExtension;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ReuseKafkaContainerExtension.class)
class ConsumerServiceTest {

    @ReusableKafkaContainer
    private KafkaContainer kafkaContainer;

    @Test
    void getRecordsCount() {
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        String topicName = "consume-topic";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        producer.send(new ProducerRecord<>(topicName, "Hello from test!"));
        producer.close();

        ConsumerService consumerService = new ConsumerService(bootstrapServers, topicName);
        int count = consumerService.getRecordsCount();

        assertEquals(1, count);
    }
}