package com.thread2.Consumer;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConsumerHandler implements Runnable {

    private final ConsumerRecords<String, String> records;
    private ThreadLocal<Offset> offsets;
    private final LinkedBlockingQueue<Offset> offsetQueue;
    AtomicBoolean isDone = new AtomicBoolean(false);
    Offset offset ;
    private Logger log = LoggerFactory.getLogger("ConsumerLog");

    public ConsumerHandler(ConsumerRecords<String, String> records,LinkedBlockingQueue offsetQueue) {
        this.records = records;
        this.offsetQueue = offsetQueue;
        this.offset = new Offset();
        this.offsets = new ThreadLocal<>();
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            Map<List<String>,Offset> recordMap = offset.getRecordListAndOffset(records,partition);
            for(List<String> recordList:recordMap.keySet()) {
                try {
                    isDone.set(DBOperation.getInstance().InsertToInfluxDB(recordList));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (isDone.get()) {
                    offsets.set(recordMap.get(recordList));
                    System.out.println(Thread.currentThread().getName() + "已将"+offsets.get().toString()+"加入offsetQueue");
                }
            }
            offsetQueue.add(offsets.get());
        }
        System.out.println(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为:" + offsetQueue.size());
        log.info(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为[{}]", offsetQueue.size());
    }
}