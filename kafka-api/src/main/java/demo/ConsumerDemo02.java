package demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static demo.DemoConsts.TEST_TOPIC;
import static demo.DemoConsts.consumerProps;

/**
 * Manual Offset Control.
 */
public class ConsumerDemo02 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo02.class);

    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(Arrays.asList(TEST_TOPIC));

        // 最小的缓存数量
        final int minBatchSize = 20;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

        int counter = 0;
        // 方式1：使用poll方法拉取数据
        while (true) {
            logger.info("Start poll for {} times", ++counter);
            // poll(timeout)的时间单位是毫秒
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }

            if (buffer.size() >= minBatchSize) {
                insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    private static void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {
        logger.info("Buffer size: {}", buffer.size());
    }

}
