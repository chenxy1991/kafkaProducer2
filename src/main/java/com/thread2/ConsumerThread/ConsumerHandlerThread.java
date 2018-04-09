package com.thread2.ConsumerThread;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


public class ConsumerHandlerThread implements Runnable {

    private final ConsumerRecords<String, String> records;
    private final Map<TopicPartition, Offset> offsets;
    private final LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue;
    AtomicBoolean isDone = new AtomicBoolean(false);
    Offset offset ;
    private Logger log = LoggerFactory.getLogger("ConsumerLog");


    public ConsumerHandlerThread(ConsumerRecords<String, String> records, Map<TopicPartition, Offset> offsets, LinkedBlockingQueue offsetQueue) {
        this.records = records;
        this.offsets = offsets;
        this.offsetQueue = offsetQueue;
        this.offset = new Offset();
    }

    public List<String> getRecordList(ConsumerRecords<String, String> records,TopicPartition partition){
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        List<String> recordList = new ArrayList<String>();
        for (ConsumerRecord<String, String> record : partitionRecords) {
            recordList.add(record.value() + "&" + record.offset());
        }
        return recordList;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<String> recordList = getRecordList(records,partition);
            System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
            log.info(Thread.currentThread().getName() + "获取数据[{}]条", recordList.size());
            try {
                isDone.set(DBOperation.getInstance().InsertToInfluxDB(recordList));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(isDone.get()){
               Offset result = offset.dealRecords(recordList);
               offsets.put(partition,result);
               System.out.println(Thread.currentThread().getName() + "----" + offsets.get(partition).toString());
            }
        }
        offsetQueue.add(offsets);
        System.out.println(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为:" + offsetQueue.size());
        log.info(Thread.currentThread().getName() + "执行完后，当前offsetQueue的大小为[{}]", offsetQueue.size());
    }
}