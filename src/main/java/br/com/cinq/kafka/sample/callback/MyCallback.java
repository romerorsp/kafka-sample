package br.com.cinq.kafka.sample.callback;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Callback;

@Component
public class MyCallback implements Callback {
    static Logger logger = LoggerFactory.getLogger(MyCallback.class);

    private static List<String> messages;

    private static long lastUpdate;

    @Override
    public void receive(String message) {
        if ((System.currentTimeMillis() - lastUpdate) > 3000L) {
            messages = new LinkedList<String>();
        }
        lastUpdate = System.currentTimeMillis();

        logger.info("Message received: {}", message);
        messages.add(message);
    }

    public static List<String> getMessages() {
        return messages;
    }

    public static void setMessages(List<String> messages) {
        MyCallback.messages = messages;
    }

}
