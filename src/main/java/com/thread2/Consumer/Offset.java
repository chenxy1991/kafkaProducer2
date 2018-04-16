package com.thread2.Consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class Offset{

     /*
       initOffset 第一条记录的位移
       lastOffset 最后一条记录的位移
       partition  插入的分区
       consumer   对应的消费者
     */
    private long initOffset;
    private long lastOffset;
    private TopicPartition partition;
    private Consumer<String, String> consumer;

    public Offset(){}

    public Offset(Consumer<String, String> consumer){
        this.consumer=consumer;
    }

    public Offset(TopicPartition partition,long initOffset,long lastOffset){
        this.initOffset = initOffset;
        this.lastOffset = lastOffset;
        this.partition = partition;
    }

    public TopicPartition getPartition() {
        return this.partition;
    }

    public long getInitOffset() {
        return this.initOffset;
    }

    public long getLastOffset() {
        return this.lastOffset;
    }

    @Override
    public String toString(){
          return "["+"partition:"+this.partition.partition()+","+this.getInitOffset() +"," + this.getLastOffset()+"]";
    }

    //当前获取的记录列表为key，将当前记录列表对应的位移的初始值，最后的位移值，以及分区作为Offset类型的value存到map中
    public Map<List<String>,Offset> getRecordListAndOffset(ConsumerRecords<String, String> records,TopicPartition partition){
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        Map<List<String>,Offset> recordsAndOffset = new HashMap<>();
        List<String> recordList=new ArrayList<String>();
        Offset offset = new Offset(partition,partitionRecords.get(0).offset(),partitionRecords.get(partitionRecords.size()-1).offset());
        for (ConsumerRecord<String, String> record : partitionRecords) {
            recordList.add(record.value());
        }
        System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
        recordsAndOffset.put(recordList,offset);
        return recordsAndOffset;
    }

    //获取partition上次提交的位移，先从kafka中获取，若获取不到则读文件，如果读文件失败，则从位移为0开始消费
    public long getLastCommited(TopicPartition partition) {
        long finalOffset = 0L;
        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
        if (offsetAndMetadata != null) {
            finalOffset = offsetAndMetadata.offset();
            System.out.println("上次提交的offset是：" + finalOffset);
        } else {
            finalOffset = Utils.readFromFile(partition, "offset.txt");
        }
        return finalOffset;
    }

    //遍历offsetQueue获取partition中最小的已消费offset
    public long getMinOffset(TopicPartition partition,LinkedBlockingQueue<Offset> offsetQueue) {
        long lastOffset = 0L,minOffset = Long.MAX_VALUE;
        for (Offset offsets : offsetQueue) {
            if(offsets.getPartition().equals(partition)){
                lastOffset = offsets.getLastOffset();
                System.out.println("lastoffset是：" + lastOffset);
                if (lastOffset < minOffset)
                    minOffset = lastOffset;
            }
        }
        return minOffset;
    }

    //将当前消费到的offset进行提交
    public void commitOffset(TopicPartition partition,long commitOffset){
        OffsetAndMetadata commitOffsetAndMetadata = new OffsetAndMetadata(commitOffset + 1);
        System.out.println("此次要提交的offset是:" + commitOffset);
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        commitMap.put(partition, commitOffsetAndMetadata);
        consumer.commitSync(commitMap);
        Utils.saveToFile(commitMap, "offset.txt");
    }

}
