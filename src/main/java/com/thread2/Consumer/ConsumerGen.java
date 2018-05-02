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
    private String[] topic;
    private Logger log = LoggerFactory.getLogger("ConsumerLog");
    InfluxDB influxDB;
    AtomicBoolean isRunning;
    Offset offset;
    final long awaitTime = 5 * 1000;

    public ConsumerGen(String[] topics, InfluxDB influxDB, Consumer<String, String> consumer) {
        this.topic = topics;
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
          for(String topic : topic) {
              offset.updateOffsetByTopic(force,topic,offsetQueue);
          }
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

