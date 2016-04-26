package br.com.cinq.kafka.sample.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import br.com.cinq.kafka.sample.Callback;
import br.com.cinq.kafka.sample.Consumer;

/**
 * Implements the loop to receive messages and call back the user operations.
 */
@Component
public class BrokerConsumer implements Consumer, DisposableBean {

   public static String TXID = "txid";

   /** Concurrent threads reading messages */
   private int partitions;

   /** Topic for subscribe, if applicable */
   private String topic;

   /** Kafka server */
   private String bootstrapServer;

   /** Group Id */
   private String groupId;

   /** Consumer class */
   private Callback callback;

   /** enableAutoCommit */
   private boolean enableAutoCommit = true;

   /** auto.commit.interval.ms */
   private int autoCommitInterval = 1000;

   /** session.timeout.ms */
   private int sessionTimeout = 30000;

   /** List of consumers */
   Thread consumers[];

   public int getPartitions() {
      return partitions;
   }

   public void setPartitions(int partitions) {
      this.partitions = partitions;
   }

   public String getTopic() {
      return topic;
   }

   public void setTopic(String topic) {
      this.topic = topic;
   }

   /**
    * Start to receive messages
    */
   public void start() {

      Properties props = new Properties();
      props.put("bootstrap.servers", getBootstrapServer());
      props.put("group.id", getGroupId());
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

      consumers = new Thread[getPartitions()];

      for (int i = 0; i < getPartitions(); i++) {
         KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
         consumer.subscribe(Arrays.asList(getTopic()+"-"+i));

//         TopicPartition partition = new TopicPartition(getTopic(), i);
//         consumer.assign(Arrays.asList(partition));

         BrokerConsumerClient client = new BrokerConsumerClient();
         client.setCallback(callback);
         client.setConsumer(consumer);

         consumers[i] = new Thread(client);
         consumers[i].setName(getBootstrapServer()+":"+i);
         consumers[i].start();
      }
   }

   public String getBootstrapServer() {
      return bootstrapServer;
   }

   public void setBootstrapServer(String bootstrapServer) {
      this.bootstrapServer = bootstrapServer;
   }

   public String getGroupId() {
      return groupId;
   }

   public void setGroupId(String groupId) {
      this.groupId = groupId;
   }

   public Callback getCallback() {
      return callback;
   }

   public void setCallback(Callback callback) {
      this.callback = callback;
   }

   @Override
   public void destroy() throws Exception {
      if(consumers!=null) {
         for(Thread t : consumers) {
            t.interrupt();
         }
      }
   }

   public boolean getEnableAutoCommit() {
      return enableAutoCommit;
   }

   public void setEnableAutoCommit(boolean enableAutoCommit) {
      this.enableAutoCommit = enableAutoCommit;
   }

   public int getAutoCommitInterval() {
      return autoCommitInterval;
   }

   public void setAutoCommitInterval(int autoCommitInterval) {
      this.autoCommitInterval = autoCommitInterval;
   }

   public int getSessionTimeout() {
      return sessionTimeout;
   }

   public void setSessionTimeout(int sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
   }
}
