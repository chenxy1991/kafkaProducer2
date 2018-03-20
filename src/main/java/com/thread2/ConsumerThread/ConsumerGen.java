package com.thread2.ConsumerThread;

import com.cxy.Consumer.KConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerGen {

    private final Consumer<String, String> consumer;
    private ExecutorService executor;
    private String topic;
    InfluxDB influxDB;
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    AtomicBoolean isRunning;
    final long awaitTime = 5 * 1000;
    int threadNum = 5;

   public ConsumerGen(String topic,InfluxDB influxDB,Consumer<String, String> consumer){
        this.topic=topic;
        this.influxDB=influxDB;
        this.consumer=consumer;
        consumer.subscribe(Arrays.asList(topic));
    }

    public void start(int threadNum) {
        this.isRunning.set(true);
        LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>>();
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
       // Result result=new Result();
        while(isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                Map<TopicPartition, OffsetAndMetadata> offsets=getOffsets(records);
                if (offsets != null) {
                    executor.submit(new ConsumerHandlerThread(records, offsets, offsetQueue));
                }
                commitOffsets(false,offsetQueue);
            } catch (Exception e) {
                commitOffsets(true,offsetQueue);
                isRunning.set(false);
            }
        }
        shutdown();
    }

    Map<TopicPartition, OffsetAndMetadata> getOffsets(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return null;
        }
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (TopicPartition partition : records.partitions()) {
            System.out.println(consumer.committed(partition));
            offsets.put(partition, consumer.committed(partition));
        }
        return offsets;
    }


    void commitOffsets(Boolean force,LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsetQueue) {
        //尽可能多的commit
        //判断offsetQueue足够大  force==true的时候不判断
        //够大则批量拿一次offset
        //取每个topic最小的提交commit
        //如果offset不连续 例如 1 3 4 则提交1  3 4 继续塞回队列
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<TopicPartition,OffsetAndMetadata> commitMap=new HashMap<>();
        long minoffset=Long.MAX_VALUE;
        for(PartitionInfo s : partitionInfos){
            TopicPartition partition=new TopicPartition(topic,s.partition());
            for(Map<TopicPartition,OffsetAndMetadata> offsets : offsetQueue){
                OffsetAndMetadata offsetAndMetadata=offsets.get(partition);
                long offset=offsetAndMetadata.offset();
                if(offset<minoffset)
                    minoffset=offset;
            }
            OffsetAndMetadata minOffsetAndMetadata=new OffsetAndMetadata(minoffset);
            commitMap.put(partition,minOffsetAndMetadata);
            }
            if(force.equals(true)) {
                consumer.commitSync(commitMap);
            }
            else{
               if(offsetQueue.size()>=2){
                   consumer.commitSync(commitMap);
                   offsetQueue.clear();
                   offsetQueue.add(commitMap);
               }
        }
    }

    public void shutdown(){
        System.out.println("consumerGen正在关闭。。。");
        try{
            executor.shutdown();
            if(!executor.awaitTermination(awaitTime, TimeUnit.MILLISECONDS)){
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.out.println("awaitTermination interrupted: " + e);
            executor.shutdownNow();
        }finally {
            influxDB.close();
            consumer.close();
            offsets.clear();
        }
    }

    public void stop() {
        this.isRunning.set(false);
    }
}

