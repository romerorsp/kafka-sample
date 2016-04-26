package br.com.cinq.kafka.sample.kafka;

import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import br.com.cinq.kafka.sample.Callback;

public class BrokerConsumerClient implements Runnable {
   Logger logger = LoggerFactory.getLogger(BrokerConsumerClient.class);

   private KafkaConsumer<String, String> consumer;

   private Callback callback;

   @Override
   public void run() {

      UUID uuid = UUID.randomUUID();
      MDC.put(consumer.toString(), uuid.toString());

      try {
         while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records != null) {
               for (ConsumerRecord<String, String> record : records) {
                  logger.debug("offset = {}, key = {}, value = {}", record.offset(), record.key(), record.value());
                  callback.receive(record.value());
               }
            }
         }
      } finally {
         MDC.remove(consumer.toString());
      }
   }

   public KafkaConsumer<String, String> getConsumer() {
      return consumer;
   }

   public void setConsumer(KafkaConsumer<String, String> consumer) {
      this.consumer = consumer;
   }

   public Callback getCallback() {
      return callback;
   }

   public void setCallback(Callback callback) {
      this.callback = callback;
   }
}
