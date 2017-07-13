package demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static demo.DemoConsts.*;

/**
 * 入门示例
 */
public class ConsumerDemo01 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo01.class);

    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(Arrays.asList(TEST_TOPIC));

        int counter = 0;
        while (counter++ < 100) {
            logger.info("Start poll for {} times", ++counter);
            // poll(timeout)的时间单位是毫秒
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("offset : {}, key : {}, value = {}, timestamp = {}",
                        record.offset(), record.key(), record.value(), record.timestamp());
            }
        }
    }
}
