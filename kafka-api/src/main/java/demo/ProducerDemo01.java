package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static demo.DemoConsts.*;

/**
 * 入门示例
 */
public class ProducerDemo01 {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemo01.class);

    public static void main(String[] args) {
        // Producer是线程安全的，在多个线程间共享一个Producer实例通常可提高效率
        Producer<String, String> producer = new KafkaProducer<>(producerProps());

        int random = new Random().nextInt(100);

        for (int i = 0; i < 10; i++) {
            // send()方法是异步操作. 调用之后消息被放入一个缓存队列并立即返回，有一个后台线程负责将消息批量发送到Broker
            logger.info("Start sending message: {}", i);
            producer.send(new ProducerRecord<>(TEST_TOPIC, Integer.toString(i), random + ":" + Integer.toString(i)));
            logger.info("End sending message: {}", i);
        }

        // Producer实例使用完必须关闭，否则会导致资源释放问题
        producer.close();
    }
}
