package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static demo.AppConfig.TOPIC_FOR_TEST;

/**
 * Kafka Producer controller
 */
@RestController
public class ProducerController {
    private static Logger logger = LoggerFactory.getLogger(ProducerController.class);

    @Autowired
    private KafkaTemplate<Integer, String> template;

    @GetMapping("send")
    public String test(@RequestParam int count, @RequestParam String prefix) {
        for (int i = 0; i < count; i++) {
            template.send(TOPIC_FOR_TEST, Integer.valueOf(i), prefix + ":" + String.valueOf(i));
            logger.info("Success send msg: {}", prefix + ":" + String.valueOf(i));
        }

        return "Success send msg to topic";
    }
}
