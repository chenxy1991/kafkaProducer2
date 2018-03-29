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


public class ConsumerHandlerThread implements Runnable {

    private final ConsumerRecords<String, String> records;
    private final Map<TopicPartition, Offset> offsets;
    private final LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue;
    private static Logger log = LoggerFactory.getLogger(ConsumerHandlerThread.class);


    public ConsumerHandlerThread(ConsumerRecords<String, String> records,Map<TopicPartition, Offset> offsets, LinkedBlockingQueue offsetQueue) {
       this.records = records;
       this.offsets=offsets;
       this.offsetQueue=offsetQueue;
    }

    @Override
    public void run() {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            List<String> recordList = new ArrayList<String>();
            for (ConsumerRecord<String, String> record : partitionRecords) {
                recordList.add(record.value() + "&" + record.offset());
            }
            System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
            Offset result = null;
            try {
                result = DBOperation.getInstance().InsertToInfluxdb(recordList);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (result != null) {
                  offsets.put(partition,result);
                  System.out.println(Thread.currentThread().getName()+"----"+offsets.get(partition).toString());
            }
        }
        offsetQueue.add(offsets);
        System.out.println(Thread.currentThread().getName()+"执行完后，当前offsetQueue的大小为:"+offsetQueue.size());
        }
}