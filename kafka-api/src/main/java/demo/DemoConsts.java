package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Demo constants.
 */
public class DemoConsts {
    // public static final String ZK_NODES = "192.168.9.79:12181,192.168.9.81:12181,192.168.9.82:12181";
    public static final String KAFKA_NODES = "192.168.9.79:19092,192.168.9.81:19092,192.168.9.82:19092";

    public static final String TEST_TOPIC = "test-04";

    private static Logger logger = LoggerFactory.getLogger(DemoConsts.class);

    public static Properties producerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_NODES);
        // acks = all表示所有ISR都反馈之后再应答，可以保证消息不会丢失但效率低
        // 在《Kafka官网》示例和《使用 Apache Kafka 进行关键业务消息传输》中，都将acks设置为all
        props.put("acks", "all");
        // Producer后台线程发送失败后的尝试重发次数
        // 在Kafka官网示例Demo中，retries设置为0
        // 在《使用 Apache Kafka 进行关键业务消息传输》中建议设置为5
        props.put("retries", 5);
        // 针对每个Partition未发送的消息允许开辟的缓存大小上限
        props.put("batch.size", 16384);
        // 消息停留在缓存区的时间，加大此时间可以增加每次发送的消息数量
        props.put("linger.ms", 1);
        // Producer允许使用的缓存总量上限
        props.put("buffer.memory", 33554432);
        // 为保证In-order delivery（消息有序），该配置来自《使用 Apache Kafka 进行关键业务消息传输》
        props.put("max.in.flight.requests.per.connection", 1);
        // key序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 为了演示方便，设置超时时间，默认是30s
        props.put("request.timeout.ms", 1000);
        props.put("timeout.ms", 1000);

        logger.debug("props: {}", props);

        return props;
    }

    public static Properties consumerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_NODES);
        // All consumer instances sharing the same group.id will be part of the same consumer group
        props.put("group.id", "test");
        // 设置 `enable.auto.commit` 为false意味着需手动commit offset
        props.put("enable.auto.commit", false);
        // key反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        logger.debug("Consumer props: {}", props);

        return props;
    }

}
