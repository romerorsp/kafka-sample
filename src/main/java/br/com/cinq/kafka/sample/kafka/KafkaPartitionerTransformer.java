package br.com.cinq.kafka.sample.kafka;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component(value="partitioner")
public class KafkaPartitionerTransformer {

    private static int rr = -1;
    private static final int ROUND_ROBBIN_FACTOR = 3;

    public Message<String> roundRobbin(Message<String> message) {
        rr = (rr+1)%ROUND_ROBBIN_FACTOR;
        System.out.println("HEADER FOR P" + rr);
        return MessageBuilder.withPayload(message.getPayload().concat(" - P"+rr)).setHeader("partitionId", rr).build();
    }
}
