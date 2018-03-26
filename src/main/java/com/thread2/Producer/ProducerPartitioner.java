package com.thread2.Producer;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class ProducerPartitioner implements Partitioner {

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
       String timestamp = o.toString().split("_")[0];
       long time=Long.parseLong(timestamp);
       System.out.println(time);
       int numPartition=cluster.partitionCountForTopic();
       return (int)(time % numPartition);
    }

    @Overrides
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
