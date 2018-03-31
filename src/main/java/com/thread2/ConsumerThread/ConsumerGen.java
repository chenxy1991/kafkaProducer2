package com.thread2.ConsumerThread;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerGen {

    private final Consumer<String, String> consumer;
    private ExecutorService executor;
    private String topic;
    //private static Logger log = LoggerFactory.getLogger(ConsumerGen.class);
    private Logger log = LoggerFactory.getLogger("ConsumerLog");
    InfluxDB influxDB;
    AtomicBoolean isRunning;
    Map<TopicPartition,Long> lastCommited=new HashMap<>();
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
                log.info("获取到的数据有[{}]条",records.count());
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
        log.info("进入commitOffsets方法...");
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
        List<Map<TopicPartition,Offset>> commitList = new ArrayList<Map<TopicPartition,Offset>>();
        long minOffset = Long.MAX_VALUE;
        long initOffset = 0L;
        long lastOffset = 0L;
        long finalOffset =2751L;
        for (PartitionInfo s : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, s.partition());
            if(lastCommited.get(partition) == null) {
               finalOffset=getLastCommited(partition);
            }
            else {
                finalOffset = lastCommited.get(partition);
            }
            if (force.equals(true)) {
                for (Map<TopicPartition, Offset> offsets : offsetQueue) {
                    if(offsets.get(partition) != null) {
                        initOffset = offsets.get(partition).getInitOffset();
                        System.out.println("initoffset是："+initOffset);
                        log.info("initoffset是[{}]",initOffset);
                        lastOffset = offsets.get(partition).getLastOffset();
                        System.out.println("lastoffset是："+lastOffset);
                        log.info("lastoffset是[{}]",lastOffset);
                        if (lastOffset < minOffset)
                            minOffset = lastOffset;
                    }
                }
                OffsetAndMetadata minOffsetAndMetadata = new OffsetAndMetadata(minOffset+1);
                System.out.println("此次要提交的offset是:"+ minOffset);
                log.info("此次要提交的offset是[{}]",minOffset);
                commitMap.put(partition, minOffsetAndMetadata);
                consumer.commitSync(commitMap);
                saveToFile(commitMap,"/offset.txt");
            } else {
                if (offsetQueue.size() >= 2) {
                    while (!offsetQueue.isEmpty()) {
                        System.out.println("当前offsetQueue的大小是："+offsetQueue.size());
                        log.info("当前offsetQueue的大小是[{}]",offsetQueue.size());
                        Map<TopicPartition, Offset> offsets = offsetQueue.poll();
                        System.out.println("poll后offsetQueue的大小是:"+offsetQueue.size());
                        log.info("poll后offsetQueue的大小是[{}]",offsetQueue.size());
                        initOffset = offsets.get(partition).getInitOffset();
                        System.out.println("initoffset是："+initOffset);
                        log.info("initoffset是[{}]",initOffset);
                        lastOffset = offsets.get(partition).getLastOffset();
                        System.out.println("lastoffset是："+lastOffset);
                        log.info("lastoffset是[{}]",lastOffset);
                        if (initOffset == finalOffset) {
                            //commitQueue.add(offsets);
                            System.out.println("true");
                            finalOffset = lastOffset;
                        } else if(lastOffset < finalOffset) {
                            offsetQueue.remove(offsets);
                        }
                        else{
                            commitList.add(offsets);
                        }
                    }
                    offsetQueue.addAll(commitList);
                    System.out.println("处理后的offsetQueue的大小为"+offsetQueue.size());
                    log.info("处理后的offsetQueue的大小为[{}]",offsetQueue.size());
                    OffsetAndMetadata minOffsetAndMetadata = new OffsetAndMetadata(finalOffset+1);
                    lastCommited.put(partition,finalOffset+1);
                    System.out.println("下一次要处理的offset是:"+(finalOffset+1));
                    log.info("下一次要处理的offset是[{}]",(finalOffset+1));
                    commitMap.put(partition, minOffsetAndMetadata);
                    consumer.commitSync(commitMap);
                    commitList.clear();
                }
            }
        }
    }

    public long getLastCommited(TopicPartition partition) {
        long finalOffset=0L;
        OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
        if(offsetAndMetadata != null) {
            //lastcommited.set(offsetAndMetadata.offset());
            //commited.set(3803L);
            lastCommited.put(partition,offsetAndMetadata.offset());
            finalOffset = offsetAndMetadata.offset();
            System.out.println("上次提交的offset是："+finalOffset);
            log.info("上次提交的offset是[{}]",finalOffset);
        }
        else{
            finalOffset= readFromFile(partition,"/offset.txt");
        }
        return finalOffset;
    }

    public void shutdown(){
        System.out.println("consumerGen正在关闭。。。");
        log.info("consumerGen正在关闭。。。");
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

    public long readFromFile(TopicPartition partition,String filename){
        BufferedReader br = null;
        String str=null;
        long offset=0L;
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
        try {
            br = new BufferedReader(new FileReader(file));
            while ((str = br.readLine())!= null) // 判断最后一行不存在，为空结束循环
            {
                if(String.valueOf(partition.partition()).equals(str.split(":")[0])) {
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

    public static void saveToFile(Map<TopicPartition,OffsetAndMetadata> commitMap, String filename) {
        BufferedWriter Buff = null;
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
        try {
            Buff = new BufferedWriter(new FileWriter(file, false));
            for (TopicPartition partition: commitMap.keySet()) {
                Buff.write(partition.partition() + ":" + String.valueOf(commitMap.get(partition)));
                Buff.write("\n");
            }
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

