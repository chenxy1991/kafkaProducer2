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
    Map<TopicPartition,Map<TopicPartition, OffsetAndMetadata>> saveMap =new HashMap<>();
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
        LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<Offset>(); //offsetQueue存放每个线程每次处理完的record的Offset的状态
        //创建含有threadNum大小核心线程的线程池
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        while (isRunning.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);  //Consumer到kafka拉取消息,每隔一秒拉取一次
                System.out.println("本次拉取到的数据有:" + records.count()+"条");
                log.info("获取到的数据有[{}]条", records.count());
                if(!records.isEmpty()){
                    executor.submit(new ConsumerHandler(records,offsetQueue));  //将该次获取的记录提交给线程池中ConsumerHandler类型的线程去处理
                }
                commitOffsets(false, offsetQueue);    //处理offsetQueue队列
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

      void commitOffsets(Boolean force, LinkedBlockingQueue<Offset> offsetQueue) {
        System.out.println("进入commitOffsets方法...");
        log.info("进入commitOffsets方法...");
        long finalOffset = 0L,minOffset = 0L;
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);   //获取topic对应的所有partition的信息
        List<Offset> commitList = new ArrayList<Offset>();
        for (PartitionInfo s : partitionInfos) {
            TopicPartition partition = new TopicPartition(topic, s.partition());
            System.out.println("现在处理的是partition:"+partition.partition());
            log.info("现在处理的是partition[{}]:",partition.partition());
            if (lastCommited.get(partition) == null) {
                finalOffset = offset.getLastCommited(partition);             //获取上次该partition提交的offset
            } else {
                finalOffset = lastCommited.get(partition);
            }
            log.info("partition[{}]上次提交的lastCommited是[{}]:",partition.partition(),finalOffset);
            if (force.equals(true)) {                                       //若force为true，表示出现了exception，则将当前队列中的所有元素进行处理，遍历offsetQueue，将所有相同的partition中对应的offset取最小值进行提交
                minOffset = offset.getMinOffset(partition, offsetQueue);
                saveMap.put(partition,offset.commitOffset(partition,minOffset));
                lastCommited.put(partition, minOffset);
            } else {
                if (offsetQueue.size() >= 2) {                              //若force为false，则当offsetQueue大小超过2时处理一次
                    finalOffset = dealOffsetQueue(partition, commitList, offsetQueue, finalOffset);
                    saveMap.put(partition,offset.commitOffset(partition, finalOffset));
                    System.out.println("saveList的大小为："+saveMap.size()+",当前saveList为："+saveMap.toString());
                    lastCommited.put(partition,finalOffset);
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

    //关闭程序
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

