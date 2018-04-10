package com.thread2.Consumer;


import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerGen {

    private final Consumer<String, String> consumer;
    private ExecutorService executor;
    private String topic;
    private Logger log = LoggerFactory.getLogger("ConsumerLog");
    InfluxDB influxDB;
    AtomicBoolean isRunning;
    Offset offset;
    Map<TopicPartition, Long> lastCommited = new HashMap<>();
    final long awaitTime = 5 * 1000;

    public ConsumerGen(String topic, InfluxDB influxDB, Consumer<String, String> consumer) {
        this.topic = topic;
        this.influxDB = influxDB;
        this.consumer = consumer;
        this.offset = new Offset(consumer);
        isRunning = new AtomicBoolean(false);
        consumer.subscribe(Arrays.asList(topic));
    }

    public void start(int threadNum) {
        this.isRunning.set(true);
        LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<Map<TopicPartition, Offset>>();
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        while (isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                System.out.println("获取到的数据有:" + records.count());
                log.info("获取到的数据有[{}]条", records.count());
                if(!records.isEmpty()){
                    executor.submit(new ConsumerHandler(records,offsetQueue));
                }
                commitOffsets(false, offsetQueue);
            } catch (Exception e) {
                commitOffsets(true, offsetQueue);
                isRunning.set(false);
            }
        }
        shutdown();
    }


/*
        //尽可能多的commit
        //判断offsetQueue足够大  force==true的时候不判断
        //够大则批量拿一次offset
        //取每个topic最小的提交commit
        //如果offset不连续 例如 1 3 4 则提交1  3 4 继续塞回队列
 */

    void commitOffsets(Boolean force, LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue) {
        System.out.println("进入commitOffsets方法...");
        log.info("进入commitOffsets方法...");
        long finalOffset = 0L,minOffset = 0L;
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        List<Map<TopicPartition, Offset>> commitList = new ArrayList<Map<TopicPartition, Offset>>();
        for (PartitionInfo s : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, s.partition());
            if (lastCommited.get(partition) == null) {
                finalOffset = offset.getLastCommited(partition);
            } else {
                finalOffset = lastCommited.get(partition);
            }
            if (force.equals(true)) {
                minOffset = offset.getMinOffset(partition, offsetQueue);
                offset.commitOffset(partition,minOffset);
                lastCommited.put(partition, minOffset + 1);
            } else {
                if (offsetQueue.size() >= 2) {
                    finalOffset = dealOffsetQueue(partition, commitList, offsetQueue, finalOffset);
                    offset.commitOffset(partition, finalOffset);
                    lastCommited.put(partition, finalOffset + 1);
                    commitList.clear();
                }
            }
        }
    }

    public long dealOffsetQueue(TopicPartition partition, List<Map<TopicPartition, Offset>> commitList,LinkedBlockingQueue<Map<TopicPartition, Offset>> offsetQueue,long finalOffset){
        long initOffset = 0L,lastOffset = 0L;
        while (!offsetQueue.isEmpty()) {
            System.out.println("当前offsetQueue的大小是：" + offsetQueue.size());
            log.info("当前offsetQueue的大小是[{}]", offsetQueue.size());
            Map<TopicPartition, Offset> offsets = offsetQueue.poll();
            System.out.println("poll后offsetQueue的大小是:" + offsetQueue.size());
            log.info("poll后offsetQueue的大小是[{}]", offsetQueue.size());
            initOffset = offsets.get(partition).getInitOffset();
            lastOffset = offsets.get(partition).getLastOffset();
            System.out.println("initoffset是："+initOffset + ",lastoffset是：" + lastOffset);
            log.info("initoffset是[{}],lastoffset是[{}]",initOffset,lastOffset);
            if (initOffset == finalOffset) {
                System.out.println("true");
                finalOffset = lastOffset;
            } else if (lastOffset < finalOffset) {
                offsetQueue.remove(offsets);
            } else {
                commitList.add(offsets);
            }
        }
        offsetQueue.addAll(commitList);
        return finalOffset;
    }

    public void shutdown() {
        System.out.println("consumerGen正在关闭。。。");
        log.info("consumerGen正在关闭。。。");
        try {
            executor.shutdown();
            if (!executor.awaitTermination(awaitTime, TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.out.println("awaitTermination interrupted: " + e);
            executor.shutdownNow();
        } finally {
            influxDB.close();
            consumer.close();
        }
    }

    public void stop() {
        this.isRunning.set(false);
    }


}

