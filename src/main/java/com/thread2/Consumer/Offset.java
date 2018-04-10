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

public class Offset {

    private long initOffset;
    private long lastOffset;
    private Consumer<String, String> consumer;

    public Offset(){}

    public Offset(Consumer<String, String> consumer){
        this.consumer=consumer;
    }

    public Offset(long initOffset,long lastOffset){
        this.initOffset = initOffset;
        this.lastOffset = lastOffset;
    }

    public long getInitOffset() {
        return initOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public Map<List<String>,Offset> getRecordListAndOffset(ConsumerRecords<String, String> records,TopicPartition partition){
        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
        Map<List<String>,Offset> recordsAndOffset = new HashMap<>();
        List<String> recordList=new ArrayList<String>();
        Offset offset = new Offset(partitionRecords.get(0).offset(),partitionRecords.get(partitionRecords.size()-1).offset());
        for (ConsumerRecord<String, String> record : partitionRecords) {
            recordList.add(record.value());
        }
        System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
        System.out.println(Thread.currentThread().getName() + "插入的该批记录的offset初始值为" + offset.getInitOffset() + ",最后一条记录的偏移值为" + offset.getLastOffset());
        recordsAndOffset.put(recordList,offset);
        return recordsAndOffset;
    }

    public long readFromFile(TopicPartition partition, String filename) {
        BufferedReader br = null;
        String str = null;
        long offset = 0L;
        File file = new File(Offset.class.getResource("/"+filename).getPath());
        try {
            br = new BufferedReader(new FileReader(file));
            while ((str = br.readLine()) != null) // 判断最后一行不存在，为空结束循环
            {
                if (String.valueOf(partition.partition()).equals(str.split(":")[0])) {
                    offset = Long.parseLong(str.split(":")[1]);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }

    public void saveToFile(Map<TopicPartition, OffsetAndMetadata> commitMap, String filename) {
        BufferedWriter Buff = null;
        File file = new File(Offset.class.getResource("/"+filename).getPath());
        try {
            Buff = new BufferedWriter(new FileWriter(file, false));
            for (TopicPartition partition : commitMap.keySet()) {
                Buff.write(partition.partition() + ":" + String.valueOf(commitMap.get(partition).offset()));
                Buff.write("\n");
            }
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getLastCommited(TopicPartition partition) {
        long finalOffset = 0L;
        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
        if (offsetAndMetadata != null) {
            finalOffset = offsetAndMetadata.offset();
            System.out.println("上次提交的offset是：" + finalOffset);
        } else {
            finalOffset = readFromFile(partition, "offset.txt");
        }
        return finalOffset;
    }

    public long getMinOffset(TopicPartition partition,LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue) {
        long lastOffset = 0L,minOffset = Long.MAX_VALUE;
        for (Map<TopicPartition, Offset> offsets : offsetQueue) {
            if(offsets.containsKey(partition)){
                lastOffset = offsets.get(partition).getLastOffset();
                System.out.println("lastoffset是：" + lastOffset);
                if (lastOffset < minOffset)
                    minOffset = lastOffset;
            }
        }
        return minOffset;
    }

    public void commitOffset(TopicPartition partition,long commitOffset){
        OffsetAndMetadata commitOffsetAndMetadata = new OffsetAndMetadata(commitOffset + 1);
        System.out.println("此次要提交的offset是:" + commitOffset);
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        commitMap.put(partition, commitOffsetAndMetadata);
        consumer.commitSync(commitMap);
        saveToFile(commitMap, "offset.txt");
    }

}
