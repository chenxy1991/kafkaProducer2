package com.thread2.ConsumerThread;

import com.cxy.Consumer.KConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerGen {

    private final Consumer<String, String> consumer;
    private ExecutorService executor;
    private String topic;
    InfluxDB influxDB;
    AtomicBoolean isRunning;
    AtomicLong commited =new AtomicLong(0);
    final long awaitTime = 5 * 1000;
    int threadNum = 5;

   public ConsumerGen(String topic,InfluxDB influxDB,Consumer<String, String> consumer){
        this.topic=topic;
        this.influxDB=influxDB;
        this.consumer=consumer;
        isRunning=new AtomicBoolean(false);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void start(int threadNum) {
        this.isRunning.set(true);
        LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<Map<TopicPartition,Offset>>();
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        while(isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                System.out.println("获取到的数据有:"+records.count());
                Map<TopicPartition,Offset> offsets=getOffsets(records);
                if (offsets != null) {
                   executor.submit(new ConsumerHandlerThread(records,offsets,offsetQueue));
                }
                commitOffsets(false,offsetQueue);
            } catch (Exception e) {
                commitOffsets(true,offsetQueue);
                isRunning.set(false);
            }
        }
        shutdown();
    }

    Map<TopicPartition,Offset> getOffsets(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return null;
        }
        Map<TopicPartition, Offset> offsets = new HashMap<TopicPartition, Offset>();
        Offset offset = new Offset();
        for (TopicPartition partition : records.partitions()) {
            offsets.put(partition, null);
        }
        System.out.println("offsets为"+offsets);
        return offsets;
    }

/*
        //尽可能多的commit
        //判断offsetQueue足够大  force==true的时候不判断
        //够大则批量拿一次offset
        //取每个topic最小的提交commit
        //如果offset不连续 例如 1 3 4 则提交1  3 4 继续塞回队列
 */

    void commitOffsets(Boolean force,LinkedBlockingQueue<Map<TopicPartition,Offset>> offsetQueue) {
        System.out.println("进入commitOffsets方法...");
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        List<Map<TopicPartition,Offset>> commitQueue = new ArrayList<Map<TopicPartition,Offset>>();
        long minoffset = Long.MAX_VALUE;
        long initOffset = 0L;
        long lastOffset = 0L;
        long finalOffset =2751L;
        for (PartitionInfo s : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, s.partition());
            OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
            if(offsetAndMetadata != null) {
                commited.set(offsetAndMetadata.offset());
                //commited.set(3608L);
                System.out.println("上次提交的offset是："+commited.get());
                finalOffset = commited.get();
            }
            else{
                finalOffset= readFromFile("offset.txt");
            }
            if (force.equals(true)) {
                for (Map<TopicPartition, Offset> offsets : offsetQueue) {
                    if(offsets.get(partition) != null) {
                        initOffset = offsets.get(partition).getInitOffset();
                        System.out.println("initoffset是："+initOffset);
                        lastOffset = offsets.get(partition).getLastOffset();
                        System.out.println("lastoffset是："+lastOffset);
                        if (lastOffset < minoffset)
                            minoffset = lastOffset;
                    }
                }
                OffsetAndMetadata minOffsetAndMetadata = new OffsetAndMetadata(minoffset+1);
                System.out.println("此次要提交的offset是:"+minoffset);
                saveToFile(minoffset,"offset.txt");
                commitMap.put(partition, minOffsetAndMetadata);
                consumer.commitSync(commitMap);
            } else {
                if (offsetQueue.size() >= 2) {
                    while (!offsetQueue.isEmpty()) {
                        System.out.println("当前offsetQueue的大小是："+offsetQueue.size());
                        Map<TopicPartition, Offset> offsets = offsetQueue.poll();
                        System.out.println("poll后offsetQueue的大小是:"+offsetQueue.size());
                        initOffset = offsets.get(partition).getInitOffset();
                        System.out.println("initoffset是："+initOffset);
                        lastOffset = offsets.get(partition).getLastOffset();
                        System.out.println("lastoffset是："+lastOffset);
                        if (initOffset == finalOffset) {
                            //commitQueue.add(offsets);
                            System.out.println("true");
                            finalOffset = lastOffset;
                        } else {
                            commitQueue.add(offsets);
                        }
                    }
                    offsetQueue.addAll(commitQueue);
                    System.out.println("处理后的offsetQueue的大小为"+offsetQueue.size());
                    OffsetAndMetadata minOffsetAndMetadata = new OffsetAndMetadata(finalOffset+1);
                    System.out.println("处理后的offsetQueue中所需提交的offset是:"+finalOffset);
                    commitMap.put(partition, minOffsetAndMetadata);
                    consumer.commitSync(commitMap);
                    commitQueue.clear();
                }
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
        }
    }

    public void stop() {
        this.isRunning.set(false);
    }

    public long readFromFile(String filename){
        BufferedReader br = null;
        String str=null;
        long offset=0L;
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
        try {
            br = new BufferedReader(new FileReader(file));
            while ((str = br.readLine())!= null) // 判断最后一行不存在，为空结束循环
            {
                offset=Long.parseLong(str);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }

    public void saveToFile(long offset,String filename)  {
        BufferedWriter Buff = null;
        File file = new File(MyThread.class.getResource(filename).getPath());
        try {
            Buff =new BufferedWriter(new FileWriter(file));
            Buff.write(String.valueOf(offset));
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

