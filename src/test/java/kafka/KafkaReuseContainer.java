package kafka;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaReuseContainer {
    private final static Map<String, KafkaContainer> kafkaContainers =
            new HashMap<>();

    public static KafkaContainer reuseContainer(String name) {
        if (kafkaContainers.containsKey(name) && Objects.nonNull(kafkaContainers.get(name))) {
            return kafkaContainers.get(name);
        } else {
            KafkaContainer container = new KafkaContainer(
                    DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));
            container.start();
            kafkaContainers.put(name, container);
            return container;
        }
    }
}
