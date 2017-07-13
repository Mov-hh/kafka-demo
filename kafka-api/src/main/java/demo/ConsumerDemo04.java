package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static demo.DemoConsts.TEST_TOPIC;
import static demo.DemoConsts.consumerProps;

/**
 * Manual Partition Assignment.
 */
public class ConsumerDemo04 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo04.class);

    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());

        TopicPartition partition0 = new TopicPartition(TEST_TOPIC, 0);
        TopicPartition partition1 = new TopicPartition(TEST_TOPIC, 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        int counter = 0;
        while (true) {
            logger.info("Start poll for {} times", ++counter);
            // poll(timeout)的时间单位是毫秒
            ConsumerRecords<String, String> records = consumer.poll(1000L);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("offset : {}, key : {}, value = {}, timestamp = {}",
                        record.offset(), record.key(), record.value(), record.timestamp());
            }
        }
    }
}
