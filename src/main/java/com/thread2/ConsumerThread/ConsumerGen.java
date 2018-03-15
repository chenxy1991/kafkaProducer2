package com.thread2.ConsumerThread;

import com.cxy.Consumer.KConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class ConsumerGen {

    private final Consumer<String, String> consumer;
    private ExecutorService executor;
    private String topic;
    //public static InfluxDB influxDB = null;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    public boolean isRunning=true;
    final long awaitTime = 5 * 1000;

    public ConsumerGen(String topic){
        this.topic=topic;
        Properties props = getConsumerProperties();
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public Properties getConsumerProperties() {
        Properties props = new Properties();
        try {
            InputStream in = ConsumerGen.class.getResourceAsStream("/consumer.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }

    public void start(int threadNum) {
        /*try {
            influxDB = DBOperation.connectDB(3);
            System.out.println(influxDB);
        }catch(Exception e){
            e.printStackTrace();
        }*/
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        Result result=new Result();
        while(isRunning) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                getOffsets(records,offsets);
                if (!records.isEmpty()) {
                    Future<Result> future=executor.submit(new ConsumerHandlerThread(records,offsets,result),result);
                    System.out.println(future.get().getThreadName()+"线程执行"+future.get().isDoneFlag());
                }
                commitOffsets();
            } catch (Exception e) {
                isRunning=false;
            }
        }
        System.exit(0);
    }

    private void getOffsets(ConsumerRecords<String, String> records,Map<TopicPartition, OffsetAndMetadata> offsets){
        if(!records.isEmpty()) {
            for (TopicPartition partition : records.partitions()) {
                System.out.println(consumer.committed(partition));
                offsets.put(partition, consumer.committed(partition));
            }
        }
    }

    private void commitOffsets() {
        if (offsets.isEmpty()) {
                return;
        }
        consumer.commitSync(offsets);
        offsets.clear();
    }

    public void stop(){
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
            //influxDB.close();
            consumer.close();
            offsets.clear();
        }
    }
}

