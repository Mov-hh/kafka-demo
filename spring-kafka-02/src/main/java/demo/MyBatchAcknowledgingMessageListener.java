package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class MyBatchAcknowledgingMessageListener implements BatchAcknowledgingMessageListener {
    private static Logger logger = LoggerFactory.getLogger(MyBatchAcknowledgingMessageListener.class);

    @Override
    public void onMessage(Object data, Acknowledgment acknowledgment) {
        logger.info("Received data", data);
        acknowledgment.acknowledge();
    }
}
