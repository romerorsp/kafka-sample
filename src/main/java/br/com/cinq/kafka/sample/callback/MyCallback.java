package br.com.cinq.kafka.sample.callback;

import java.sql.Timestamp;
import java.util.Date;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.transaction.Transactional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Callback;
import br.com.cinq.kafka.sample.entity.Message;
import br.com.cinq.kafka.sample.repository.MessagesRepository;

@Component
@Scope("prototype")
public class MyCallback implements Callback {
    static Logger logger = LoggerFactory.getLogger(MyCallback.class);

    @Autowired
    MessagesRepository dao;

    @Autowired
    EntityManager em;

    @Override
    @Transactional
    public void receive(String message) {
        logger.info("Message received: {} by {}", message, Thread.currentThread().getName() + ":" + Thread.currentThread().getId());

        EntityTransaction trn = null;
        try {
            Message entity = new Message();
            entity.setMessage(message);
            entity.setCreated(new Timestamp(System.currentTimeMillis()));
            dao.save(entity);
        } catch (Exception e) {
            logger.error("Couldn't insert a message", e);
        } finally {
        }
    }
}
