package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {
    private static Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${kafka.servers:localhost}")
    private String kafkaServers;

    @PostConstruct
    public void init() {
        logger.info("KafkaConfig inited. kafka servers: {}", kafkaServers);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", kafkaServers);
        // All consumer instances sharing the same group.id will be part of the same consumer group
        props.put("group.id", "test");
        // 设置 `enable.auto.commit` 为false意味着需手动commit offset
        props.put("enable.auto.commit", false);
        // key反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", kafkaServers);
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

        return props;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(MyProducerListener myProducerListener) {
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setProducerListener(myProducerListener);
        return kafkaTemplate;
    }
}
