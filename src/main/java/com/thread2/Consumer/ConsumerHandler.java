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
    private ThreadLocal<Offset> offsets;                   //每个线程都有一个独立的offsets的副本
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
            Map<List<String>,Offset> recordMap = offset.getRecordListAndOffset(records,partition); //获取<records，Offset>的map
            for(List<String> recordList:recordMap.keySet()) {
                try {
                    isDone.set(DBOperation.getInstance().InsertToInfluxDB(recordMap));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (isDone.get()) {
                    offsets.set(recordMap.get(recordList));      //设置offsets的值
                    System.out.println(Thread.currentThread().getName() + "已将"+offsets.get().toString()+"加入offsetQueue");
                }
            }
            offsetQueue.add(offsets.get());    //将该线程所处理的offsets存到offsetQueue队列中待处理
        }
        System.out.println(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为:" + offsetQueue.size());
        log.info(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为[{}]", offsetQueue.size());
    }
}