package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
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

    public long getInitOffset() {
        return initOffset;
    }

    public void setInitOffset(long initOffset) {
        this.initOffset = initOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public Map<TopicPartition, Offset> getOffsets(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return null;
        }
        Map<TopicPartition, Offset> offsets = new HashMap<TopicPartition, Offset>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> record : partitionRecords){

            }
            offsets.put(partition, null);
        }
        return offsets;
    }

    public Offset dealRecords(List<String> records) {
        Offset offset = null;
        if (records.size() != 0) {
            long initoffset = Long.valueOf(records.get(0).split("&")[1]);
            long lastoffset = Long.valueOf(records.get(records.size() - 1).split("&")[1]);
            offset = new Offset();
            offset.setInitOffset(initoffset);
            offset.setLastOffset(lastoffset);
            System.out.println(Thread.currentThread().getName() + "插入的该批记录的offset初始值为" + offset.getInitOffset() + ",最后一条记录的偏移值为" + offset.getLastOffset());
        }
        return offset;
    }

    public long readFromFile(TopicPartition partition, String filename) {
        BufferedReader br = null;
        String str = null;
        long offset = 0L;
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
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
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
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
            finalOffset = readFromFile(partition, "/offset.txt");
        }
        return finalOffset;
    }

    public long getMinOffset(TopicPartition partition,LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue) {
        long initOffset = 0L,lastOffset = 0L,minOffset = Long.MAX_VALUE;
        for (Map<TopicPartition, Offset> offsets : offsetQueue) {
            if (offsets.get(partition) != null) {
                initOffset = offsets.get(partition).getInitOffset();
                System.out.println("initoffset是：" + initOffset);
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
        saveToFile(commitMap, "/offset.txt");
    }

}
