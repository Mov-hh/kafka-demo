package demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static demo.DemoConsts.TEST_TOPIC;
import static demo.DemoConsts.consumerProps;

/**
 * Storing Offsets Outside Kafka.
 * <p>
 * 通过指定{@link ConsumerRebalanceListener}实例，可以实现对offset的指定
 */
public class ConsumerDemo06 {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo06.class);

    // 假设此处保存的是partition 0的offset
    private static final long OFFSET = 380L;

    public static void main(String[] args) {
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(Arrays.asList(TEST_TOPIC), new ConsumerRebalanceListener() {
            /**
             * 当partition重新分配时调用
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    logger.info("Partition revoked. partition: {}", partition.partition());
                    consumer.seek(partition, OFFSET);
                }
            }

            /**
             * 当partition初始分配时调用
             * @param partitions
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    logger.info("Partition inited. partition: {}!", partition.partition());
                    consumer.seek(partition, OFFSET);
                }
            }
        });

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
