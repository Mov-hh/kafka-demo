package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static demo.DemoConsts.TEST_TOPIC;
import static demo.DemoConsts.consumerProps;

/**
 * Commit offset after finish handling the records in each partition.
 */
public class ConsumerDemo03 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo03.class);

    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(Arrays.asList(TEST_TOPIC));
        boolean running = true;
        int counter = 0;
        try {
            while(running) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        logger.info("partition: {}, record offset: {}, value {}", partition.partition(),
                                record.offset(), record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    // The committed offset should always be the offset of the next message that your application will
                    // read. Thus, when calling commitSync(offsets) you should add one to the offset of the last message
                    // processed.
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));

                    running = ++counter < 10;
                }
            }
        } finally {
            consumer.close();
        }
    }
}
