package kafka;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

import java.lang.reflect.Field;

public class ReuseKafkaContainerExtension implements TestInstancePostProcessor {

    @Override
    public void postProcessTestInstance(Object o, ExtensionContext extensionContext) throws Exception {
        Field[] fields = o.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.isAnnotationPresent(ReusableKafkaContainer.class)) {
                String containerName = field.getAnnotation(ReusableKafkaContainer.class)
                        .containerName();
                field.setAccessible(true);
                field.set(o, KafkaReuseContainer.reuseContainer(containerName));
            }
        }
    }
}
