package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import sun.awt.windows.ThemeReader;

import java.util.List;
import java.util.Map;

public class hellowordThread implements Runnable {

    Result result;
    private static InfluxDB influxDB = DBOperation.connectDB(3);
    //private final ConsumerRecords<String, String> records;

    /*public hellowordThread(ConsumerRecords<String, String> records) {
        this.records = records;

    }*/

    public hellowordThread(Result result){
           this.result=result;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + ":hello world");
        System.out.println(Thread.currentThread().getName() +influxDB);
        result.setDoneFlag(true);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.out.println(Thread.currentThread().getName() + "interrupted: " + e);
            System.out.println("保留现场");
        }catch(Exception e){
            result.setDoneFlag(false);
        }
    }
}
