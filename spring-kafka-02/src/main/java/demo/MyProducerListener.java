package demo;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.stereotype.Component;

/**
 * 自定义ProducerListener实现类
 */
@Component
public class MyProducerListener implements ProducerListener {
    private static Logger logger = LoggerFactory.getLogger(MyProducerListener.class);

    @Override
    public void onSuccess(String topic, Integer partition, Object key, Object value, RecordMetadata recordMetadata) {
        logger.info("Success. topic: {}, partition: {}, key: {}, value: {}, recordMetadata: {}",
                topic, partition, key, value, recordMetadata);
    }

    @Override
    public void onError(String topic, Integer partition, Object key, Object value, Exception exception) {
        logger.error("Error. topic: {}, partition: {}, key: {}, value: {}", topic, partition, key, value, exception);
    }

    @Override
    public boolean isInterestedInSuccess() {
        return true;
    }
}
