package br.com.cinq.kafka.sample.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import br.com.cinq.kafka.sample.Callback;

public class MyCallback implements Callback {
    static Logger logger = LoggerFactory.getLogger(MyCallback.class);

    @Override
    public void receive(String message) {
        
        logger.info("Message received: {}", message);
    }

}
