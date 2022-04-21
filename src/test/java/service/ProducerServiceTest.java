package service;

import kafka.ReusableKafkaContainer;
import kafka.ReuseKafkaContainerExtension;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(ReuseKafkaContainerExtension.class)
class ProducerServiceTest {

    @ReusableKafkaContainer
    private KafkaContainer kafkaContainer;

    @Test
    void sendRecords() {
        String bootstrapServers = kafkaContainer.getBootstrapServers();
        String topicName = "send-topic";

        ProducerService producerService = new ProducerService(bootstrapServers, topicName);
        producerService.sendRecords(List.of("test1", "test2", "test3", "test4"));

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "java-group-test");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topicName));
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(10000L));
        consumer.close();

        assertNotNull(consumerRecords);
        assertEquals(4, consumerRecords.count());

    }
}