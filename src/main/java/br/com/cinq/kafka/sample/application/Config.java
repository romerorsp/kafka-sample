package br.com.cinq.kafka.sample.application;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import br.com.cinq.kafka.resource.KafkaSampleResource;
import br.com.cinq.kafka.sample.Consumer;
import br.com.cinq.kafka.sample.Producer;
import br.com.cinq.kafka.sample.callback.MyCallback;
import br.com.cinq.kafka.sample.kafka.BrokerConsumer;
import br.com.cinq.kafka.sample.kafka.BrokerProducer;
import br.com.cinq.kafka.sample.mono.QueueProducerConsumer;

/**
 * Register Jersey modules
 * @author Adriano Kretschmer
 */
@Configuration
@ApplicationPath("rest")
public class Config extends ResourceConfig {

    private QueueProducerConsumer testProducerConsumer;

    public Config() {
        register(KafkaSampleResource.class);
        //		packages("br.com.cinq.greet.resource");
        //		property(ServletProperties.FILTER_FORWARD_ON_404, true);
    }

//    @Bean
//    @Profile("!unit")
//    @Qualifier("sampleProducer")
//    public Producer createBrokerProducer() {
//        BrokerProducer producer = new BrokerProducer();
//
//        return producer;
//    }
//
//    @Bean
//    @Profile("!unit")
//    @Qualifier("sampleConsumer")
//    public Consumer createBrokerConsumer() {
//
//        BrokerConsumer consumer = new BrokerConsumer();
//        return consumer;
//    }

    @Bean
    @Profile("unit")
    @Qualifier("sampleProducer")
    public Producer createProducerTest() {
        return getTestProducerConsumer();
    }

    @Bean
    @Profile("unit")
    @Qualifier("sampleConsumer")
    public Consumer createConsumerTest() {

        return getTestProducerConsumer();
    }

    private QueueProducerConsumer getTestProducerConsumer() {
        if (testProducerConsumer == null) {
            testProducerConsumer = new QueueProducerConsumer();
            testProducerConsumer.setPartitions(5);
            testProducerConsumer.setCallback(new MyCallback());

            testProducerConsumer.start();
        }

        return testProducerConsumer;
    }

    /**
     * Either you use the bean initialization to redirect rest calls or use @ApplicationPath
    @Bean
    public ServletRegistrationBean jerseyServlet() {
        ServletRegistrationBean registration = new ServletRegistrationBean(new ServletContainer(), "/rest/*");
        registration.addInitParameter(ServletProperties.JAXRS_APPLICATION_CLASS, Config.class.getName());
        return registration;
    }
     */

}