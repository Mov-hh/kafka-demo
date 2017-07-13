package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static demo.DemoConsts.TEST_TOPIC;
import static demo.DemoConsts.consumerProps;

/**
 * Storing Offsets Outside Kafka.
 * <p>
 *     手动指定partition的情况下，以下代码可实现指定offset，但如果非指定partition的场景下，以下代码不可用，可以参考
 * </p>
 */
public class ConsumerDemo05 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo05.class);

    // 假设此处保存的是partition 0的offset
    private static final long OFFSET = 0L;

    public static void main(String[] args) {

        // This type of usage is simplest when the partition assignment is also done manually (this would be likely in
        // the search index use case described above). If the partition assignment is done automatically special care is
        // needed to handle the case where partition assignments change.

        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        TopicPartition partition0 = new TopicPartition(TEST_TOPIC, 0);
        consumer.assign(Arrays.asList(partition0));
        // seek to special offset
        consumer.seek(partition0, OFFSET);

        boolean running = true;
        int counter = 0;
        try {
            while(running) {
                ConsumerRecords<String, String> records = consumer.poll(1);
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
                    running = ++counter < 2;
                }
            }
        } finally {
            consumer.close();
        }
    }
}
