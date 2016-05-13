package br.com.cinq.kafka.sample.kafka.consumer;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;

import br.com.cinq.kafka.sample.entity.Message;
import br.com.cinq.kafka.sample.repository.MessagesRepository;

public class ConsumerPojo {

    @Autowired
    MessagesRepository dao;

//    public void consume(Object message) {
//        System.out.println("MESSAGE RECEIVED: ".concat(message.toString()));
//    }

    @Transactional
    public void consume(Object message) {
        try {
            Message entity = new Message();
            System.out.println("MESSAGE RECEIVED: ".concat(message.toString()));
            entity.setMessage(message.toString());
            dao.save(entity);
        } catch (Exception e) {
            System.out.println("Couldn't insert a message");
        }
    }
}
