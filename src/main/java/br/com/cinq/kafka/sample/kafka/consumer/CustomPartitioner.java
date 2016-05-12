package br.com.cinq.kafka.sample.kafka.consumer;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class CustomPartitioner implements Partitioner {

    private static int rrCount = -1;

    @Override
    public void configure(Map<String, ?> map) {
        System.out.println("MAP: ".concat(map.toString()));
    }

    @Override
    public void close() {
        System.out.println("I WAS ASKED TO CLOSE");
    }

    @Override
    public int partition(String topic, Object key, byte[] kbytes, Object value, byte[] vbytes, Cluster cluster) {
        return rrCount = (rrCount+1)%cluster.partitionCountForTopic(topic);
    }
}
