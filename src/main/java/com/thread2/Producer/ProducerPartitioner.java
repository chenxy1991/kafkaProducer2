package com.thread2.Producer;


import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

//Partitioner,producer在发送消息时会根据该partitioner进行对key的处理
//该partitioner采取时间戳取模的方式将消息存入不同的partition中
public class ProducerPartitioner implements Partitioner {

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
       String timestamp = o.toString().split("_")[0];  //o是消息，object类型要先转换为String类型
       Double time=Double.parseDouble(timestamp);
       int numPartition=cluster.partitionCountForTopic(s);   //s是topic
       return (int)(time % numPartition);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
