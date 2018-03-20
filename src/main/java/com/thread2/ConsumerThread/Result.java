package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Result {

    public LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>>();

    public LinkedBlockingQueue getOffsetQueue() {
        return offsetQueue;
    }

    public void setOffsetQueue(LinkedBlockingQueue offsetQueue) {
        this.offsetQueue = offsetQueue;
    }

}
