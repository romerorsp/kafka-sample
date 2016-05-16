package br.com.cinq.kafka.sample.application;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.converter.MessageConverter;

import br.com.cinq.kafka.sample.kafka.consumer.StringJsonMessageConverter;

@Configuration
public class KafkaConfig {

    @Autowired
    private ApplicationContext context;

    @Value("${broker.bootstrapServer}")
    private String brokerServers;

    @Value("${broker.partitions}")
    private int partitions;

    @Value("${broker.topic}")
    private String topic;

    /** Group Id */
    @Value("${broker.group-id}")
    private String groupId;

    @Value("${broker.consumer.enable-auto-commit:true}")
    private boolean enableAutoCommit;

    @Value("${broker.consumer.max-idle}")
    private int maxIdle;

    @Value("${broker.consumer.auto-commit-interval:1000}")
    private int autoCommitInterval;

    @Value("${broker.consumer.session-timeout}")
    private int sessionTimeout;

    @Value("${broker.consumer.request-timeout}")
    private int requestTimeout;

    @Value("${broker.producer.batch-size:16384}")
    private int batchSize;

    @Value("${broker.producer.linger-time-ms:0}")
    private int lingerTime;

    @Value("${broker.producer.buffer-size:33554432}")
    private int bufferMemory;

    @Value("${broker.producer.retries:2}")
    private int retries;

//    @Bean
//    public Producer createBrokerProducer() {
//        BrokerProducer producer = new BrokerProducer();
//
//        return producer;
//    }

    @Bean
    @SuppressWarnings("unchecked")
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(context.getBean("producerConfigs", Map.class));
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerTime);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Bean
    @SuppressWarnings("unchecked")
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<Integer, String>(context.getBean(ProducerFactory.class));
    }

//    @Bean
//    @SuppressWarnings("unchecked")
//    public SimpleKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//        SimpleKafkaListenerContainerFactory<String, String> factory = new SimpleKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(context.getBean(ConsumerFactory.class));
//        factory.setConcurrency(10);
//        factory.setAckMode(AckMode.RECORD);
//        return factory;
//    }

    @Bean
    @SuppressWarnings("unchecked")
    public AbstractMessageListenerContainer<String, String> kafkaListenerContainer() {
        ConsumerFactory<String, String> lcFactory = context.getBean(ConsumerFactory.class);
        KafkaMessageListenerContainer<String, String> listener = new KafkaMessageListenerContainer<String, String>(lcFactory, topic);
        listener.setAckMode(AckMode.RECORD);
        listener.setAutoStartup(true);
        return listener;
    }

    @SuppressWarnings("unchecked")
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(context.getBean("consumerConfigs", Map.class));
        return cf;
    }

    @Bean
    public MessageConverter messageConverter() {
        return new StringJsonMessageConverter();
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, maxIdle);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        if(enableAutoCommit) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        }
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 512);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
