package br.com.cinq.kafka.sample.kafka.consumer;

public class ConsumerPojo {
    public void consume(Object message) {
        System.out.println("MESSAGE RECEIVED: ".concat(message.toString()));
    }
}
