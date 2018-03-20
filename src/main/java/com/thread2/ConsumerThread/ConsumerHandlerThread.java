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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ConsumerHandlerThread implements Runnable {

    private final ConsumerRecords<String, String> records;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;
    private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsetQueue;
    Result result;

    private static Logger log = LoggerFactory.getLogger(ConsumerHandlerThread.class);

    public ConsumerHandlerThread(ConsumerRecords<String, String> records,Map<TopicPartition, OffsetAndMetadata> offsets, LinkedBlockingQueue offsetQueue) {
        this.records = records;
        this.offsets=offsets;//this.result=result;
        this.offsetQueue=offsetQueue;
    }

    @Override
    public void run() {
        long soffset = 0;
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            List<String> recordList = new ArrayList<String>();
            for (ConsumerRecord<String, String> record : partitionRecords) {
                recordList.add(record.value() + "," + record.offset());
            }
            System.out.println(Thread.currentThread().getName() + "获取数据" + recordList.size() + "条");
            try {
                soffset = DBOperation.getInstance().InsertToInfluxdb(recordList);
                if (soffset != -1) {
                    long curr = offsets.get(partition).offset();
                    if (curr <= soffset + 1) {
                        offsets.put(partition, new OffsetAndMetadata(soffset + 1));
                    }
              }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        offsetQueue.add(offsets);
        }
}