package com.thread2.Mytest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import sun.awt.windows.ThemeReader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class hellowordThread implements Runnable {

    //Result result;
    private final List<String> offsets;
    private final LinkedBlockingQueue<List<String>> offsetQueue;


    //  private static InfluxDB influxDB = DBOperation.connectDB(3);
    //private final ConsumerRecords<String, String> records;

    /*public hellowordThread(ConsumerRecords<String, String> records) {
        this.records = records;

    }*/

    /*public hellowordThread(Result result){
           this.result=result;
    }*/

    public hellowordThread( List<String> offsets, LinkedBlockingQueue offsetQueue){
        this.offsets=offsets;
        this.offsetQueue=offsetQueue;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + ":hello world");
        offsets.add(Thread.currentThread().getName()+":123");
        System.out.println(Thread.currentThread().getName() + offsets.toString());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println(Thread.currentThread().getName() + "interrupted: " + e);
            System.out.println("保留现场");
        }catch(Exception e){
           // result.setDoneFlag(false);
        }
        offsetQueue.add(offsets);
    }
}
