package com.thread2.ConsumerThread;

import com.alibaba.fastjson.JSONObject;
import com.cxy.Consumer.KConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ConsumerHandlerThread implements Runnable {

    private final ConsumerRecords<String, String> records;
    public static BatchPoints batchPoints = null;
    private static InfluxDB influxDB = null;
    public static String dbname = "cxy";
    private static Logger log = LoggerFactory.getLogger(ConsumerHandlerThread.class);
    public static DBOperation operation=new DBOperation();
    private final Consumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private Result result;

    public ConsumerHandlerThread(ConsumerRecords<String, String> records, InfluxDB influxDB,Consumer<String, String> consumer,Map<TopicPartition, OffsetAndMetadata> offsets,Result result) {
        this.records = records;
        this.influxDB=influxDB;
        this.consumer=consumer;
        this.offsets=offsets;
        this.result=result;
    }



    @Override
    public void run() {
        long soffset = 0;
        for (TopicPartition partition : records.partitions()) {
            System.out.println(consumer.committed(partition));
            long curoffset = consumer.committed(partition).offset();
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            List<String> recordList = new ArrayList<String>();
            for (ConsumerRecord<String, String> record : partitionRecords) {
                recordList.add(record.value() + "," + record.offset());
            }
            System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
            try {
                soffset = operation.InsertToInfluxdb(recordList);
                synchronized (offsets) {
                    if (soffset != -1) {
                        System.out.println("成功插入influxdb");
                        if (!offsets.containsKey(partition)) {
                            offsets.put(partition, new OffsetAndMetadata(soffset + 1));
                        } else {
                            long curr = offsets.get(partition).offset();
                            if (curr <= soffset + 1) {
                                offsets.put(partition, new OffsetAndMetadata(soffset + 1));
                            }
                        }
                    }
                    /*else
                        offsets.put(partition,consumer.committed(partition));*/
                }
            } catch (Exception e) {
                if (soffset == -1 && e.getMessage().equals("influxDB批量写入失败")) {
                    offsets.put(partition, consumer.committed(partition));
                    result.setDoneFlag(false);
                    result.setThreadName(Thread.currentThread().getName());
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + "interrupted: " + e);
                result.setDoneFlag(false);
                result.setThreadName(Thread.currentThread().getName());
            }
        }
    }
}