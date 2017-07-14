package demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

import static demo.AppConfig.TOPIC_FOR_TEST;

@SpringBootApplication
public class Application {

    private static Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @KafkaListener(id = "test01", topics = TOPIC_FOR_TEST, containerFactory = "batchKafkaListenerContainerFactory")
    public void listen01(List<ConsumerRecord<String, String>> list) {
        logger.info("Listen 01 received {} msg", list.size());
        for (ConsumerRecord<String, String> record : list) {
            logger.info("msg: {}", record);
        }
    }

    @KafkaListener(id = "test02", topics = TOPIC_FOR_TEST, containerFactory = "batchKafkaListenerContainerFactory02")
    public void listen02(List<ConsumerRecord<String, String>> list, Acknowledgment ack) {
        logger.info("Listen 02 received {} msg", list.size());
        for (ConsumerRecord<String, String> record : list) {
            logger.info("msg: {}", record);
        }

        ack.acknowledge();
    }
}
