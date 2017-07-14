package demo;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.TopicPartitionInitialOffset;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static demo.AppConfig.TOPIC_FOR_TEST;

@Configuration
@EnableKafka
public class ConsumerConfig {

    private static Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);

    @Value("${kafka.servers:localhost}")
    private String kafkaServers;

    @PostConstruct
    public void init() {
        logger.info("`ConsumerConfig` inited. kafkaServers: {}", kafkaServers);
    }


    @Bean
    MessageListenerContainer test() {
        KafkaListenerContainerFactory<?> factory = batchKafkaListenerContainerFactory02();
        MessageListenerContainer container = factory.createListenerContainer(null);

        return container;
    }

    @Bean
    KafkaListenerContainerFactory<?> batchKafkaListenerContainerFactory02() {

        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);

        factory.getContainerProperties().setAckMode(AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerRebalanceListener() {
            /**
             * 当partition重新分配时调用
             * @param partitions
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    logger.info("Partition revoked. partition: {}", partition.partition());
                    // 注：无法获取consumer对象，这个问题需要在新版本(2.0.0.M1)的spring-kafka中得以解决
//                    consumer.seek(partition, OFFSET);
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
                    // 注：无法获取consumer对象，这个问题需要在新版本(2.0.0.M1)的spring-kafka中得以解决
//                    consumer.seek(partition, OFFSET);
                }
            }
        });

        return factory;
    }

    @Bean
    KafkaListenerContainerFactory<?> batchKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
            kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    @Bean
    ContainerProperties containerProperties() {
        TopicPartitionInitialOffset topicPartitionInitialOffset0 = new TopicPartitionInitialOffset(TOPIC_FOR_TEST,
                0, 0L, false);
        TopicPartitionInitialOffset topicPartitionInitialOffset1 = new TopicPartitionInitialOffset(TOPIC_FOR_TEST,
                1, 0L, false);
        return new ContainerProperties(topicPartitionInitialOffset0, topicPartitionInitialOffset1);
    }

    @Bean
    ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put("bootstrap.servers", kafkaServers);
        // All consumer instances sharing the same group.id will be part of the same consumer group
        props.put("group.id", "test");
        // 设置 `enable.auto.commit` 为false意味着需手动commit offset
        props.put("enable.auto.commit", false);
        // key反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        // value反序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;
    }

}
