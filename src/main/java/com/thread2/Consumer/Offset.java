package com.thread2.Consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Map<TopicPartition, Long> lastCommited = new HashMap<>();
    Map<TopicPartition,Map<TopicPartition, OffsetAndMetadata>> saveMap =new HashMap<>();
    private Logger log = LoggerFactory.getLogger("ConsumerLog");


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

    public void setInitOffset(long initOffset) {
        this.initOffset = initOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
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
            recordList.add(record.value()+"&"+record.offset());
        }
        System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条"+",记录的offset初始值为" + offset.getInitOffset() + ",最后一条记录的偏移值为" + offset.getLastOffset());
        recordsAndOffset.put(recordList,offset);
        return recordsAndOffset;
    }

    //获取partition上次提交的位移，先从kafka中获取，若获取不到则读文件，如果读文件失败，则从位移为0开始消费
    public long getLastCommited(TopicPartition partition) {
        long finalOffset = 0L;
        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
        if (offsetAndMetadata != null) {
            finalOffset = offsetAndMetadata.offset();
            System.out.println("partition"+partition.partition()+"上次提交的offset是：" + finalOffset);
        } else {
            finalOffset = Utils.readFromFile(partition,"offset.txt");
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
    public Map<TopicPartition,OffsetAndMetadata> commitOffset(TopicPartition partition,long commitOffset){
        OffsetAndMetadata commitOffsetAndMetadata = new OffsetAndMetadata(commitOffset + 1);
        System.out.println("此次要提交的partition是"+ partition.partition()+",offset是:" + commitOffset);
        log.info("partition[{}]此次要提交的offset是:",partition.partition(),commitOffset);
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        commitMap.put(partition, commitOffsetAndMetadata);
        consumer.commitSync(commitMap);
        return commitMap;
    }

    public void updateOffsetByTopic(Boolean force,String topic,LinkedBlockingQueue<Offset> offsetQueue){
        long finalOffset = 0L,minOffset = 0L;
        List<Offset> commitList = new ArrayList<Offset>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);   //获取topic对应的所有partition的信息
        for (PartitionInfo s : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, s.partition());
            System.out.println("现在处理的是partition:" + partition.partition());
            log.info("现在处理的是partition[{}]:", partition.partition());
            if (lastCommited.get(partition) == null) {
                finalOffset = getLastCommited(partition);             //获取上次该partition提交的offset
            } else {
                finalOffset = lastCommited.get(partition);
            }
            log.info("partition[{}]上次提交的lastCommited是[{}]:", partition.partition(), finalOffset);
            if (force.equals(true)) {                                       //若force为true，表示出现了exception，则将当前队列中的所有元素进行处理，遍历offsetQueue，将所有相同的partition中对应的offset取最小值进行提交
                minOffset = getMinOffset(partition, offsetQueue);
                saveMap.put(partition, commitOffset(partition, minOffset));
                lastCommited.put(partition, minOffset);
            } else {
                if (offsetQueue.size() >= 2) {                              //若force为false，则当offsetQueue大小超过2时处理一次
                    finalOffset = dealOffsetQueue(partition, commitList, offsetQueue, finalOffset);
                    saveMap.put(partition, commitOffset(partition, finalOffset));
                    System.out.println("saveList的大小为：" + saveMap.size() + ",当前saveList为：" + saveMap.toString());
                    lastCommited.put(partition, finalOffset);
                    commitList.clear();
                }
            }
        }
        Utils.saveToFile(saveMap, "offset.txt");
    }

    public long dealOffsetQueue(TopicPartition partition, List<Offset> commitList,LinkedBlockingQueue<Offset> offsetQueue,long finalOffset){
        long initOffset = 0L,lastOffset = 0L;
        while (!offsetQueue.isEmpty()) {
            System.out.println("当前offsetQueue的大小是：" + offsetQueue.size());
            log.info("当前offsetQueue的大小是[{}]", offsetQueue.size());
            Offset offsets = offsetQueue.poll();
            System.out.println("poll后offsetQueue的大小是:" + offsetQueue.size());
            log.info("poll后offsetQueue的大小是[{}]", offsetQueue.size());
            if (offsets.getPartition().partition() == partition.partition()) {
                initOffset = offsets.getInitOffset();
                lastOffset = offsets.getLastOffset();
                System.out.println("当前处理的partition是"+partition.partition()+",initoffset是：" + initOffset + ",lastoffset是：" + lastOffset);
                log.info("当前处理的partition是[{}],initoffset是[{}],lastoffset是[{}]", partition.partition(),initOffset, lastOffset);
                if (initOffset == finalOffset || initOffset == finalOffset + 1) {     //遍历当前offsetQueue，将当前元素的初始值和当前partition上次提交的位移进行比较，若相等，将lastOffset设置为此次要提交的offset，直到不相等
                    System.out.println("true");
                    finalOffset = lastOffset;
                } else if (lastOffset < finalOffset) {          //若当前元素的lastoffset已经小于已提交的offset，则在offsetQueue中删除该元素
                    offsetQueue.remove(offsets);
                } else {
                    commitList.add(offsets);      //否则放到提交队列中，等待下一轮处理
                }
            } else {
                commitList.add(offsets);
            }
        }
        offsetQueue.addAll(commitList);
        return finalOffset;
    }


}
