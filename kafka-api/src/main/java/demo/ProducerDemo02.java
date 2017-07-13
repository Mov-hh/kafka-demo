package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static demo.DemoConsts.*;

/**
 * 指定Callback
 */
public class ProducerDemo02 {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemo01.class);

    public static void main(String[] args) {
        // Producer是线程安全的，在多个线程间共享一个Producer实例通常可提高效率
        Producer<String, String> producer = new KafkaProducer<>(producerProps());

        int random = new Random().nextInt(100);

        for (int i = 0; i < 10; i++) {
            // send()方法是异步操作. 调用之后消息被放入一个缓存队列并立即返回，有一个后台线程负责将消息批量发送到Broker
            // 匿名内部类只能访问到payload【注：final类型】，不能访问i
            final String payload = Integer.toString(i);
            logger.info("Start sending message: {}", i);
            producer.send(new ProducerRecord<>(TEST_TOPIC, Integer.toString(i), random + ":" + Integer.toString(i)),
                    (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Error callback: {}, {}", payload, metadata, exception);
                        }

                        if (metadata != null) {
                            logger.info("Success callback: {}, {}", payload, metadata);
                        }
                    });
            logger.info("End sending message: {}", i);
        }

        // Producer实例使用完必须关闭，否则会导致资源释放问题
        producer.close(1, TimeUnit.MINUTES);
    }
}
