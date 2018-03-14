package com.cxy.Consumer;

import com.alibaba.fastjson.JSONObject;
import com.cxy.Producer.KProducer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;



public class KConsumer {

    private final Consumer<String, String> consumer;
    public static InfluxDB influxDB = null;
    public static BatchPoints batchPoints = null;
    public static String dbname = "cxy";
    public static Connection conn = null;
    private static Logger log = LoggerFactory.getLogger(KConsumer.class);

    public KConsumer() {
        Properties props = new Properties();
        try {
            InputStream in = KConsumer.class.getResourceAsStream("/consumer.properties");
            props.load(in);
        }catch(Exception e){
            e.printStackTrace();
        }
        consumer = new KafkaConsumer<String, String>(props);
    }

    public void connectDB(int times) {
        try {
            connect();
        } catch (SQLException e) {
            if (e.getMessage().equals("influxDB数据库连接失败")) {
                int time=1;
                while(time<=times) {
                    try {
                        connect();
                    } catch (Exception es) {
                       es.printStackTrace();
                       time++;
                    }
                }
                consumer.close();
            }
        }
    }
    /*public void consumetest(){
        TopicPartition partition=new TopicPartition("cpu",0);
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition,1000);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            System.out.println(records.count());
            if (records.count() != 0) {
                List<String> recordList = new ArrayList<String>();
                for (ConsumerRecord<String, String> record : records) {
                    long lastoffset = record.offset();
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    log.info("offset = [{}], key = [{}], value =[{}]", record.offset(), record.key(), record.value());
                    System.out.println();
                    System.out.println(record.toString());
                    recordList.add(record.value());
                }
            }
        }
    }*/

    public void consume(){
        connectDB(3);
        int soffset = 0;
        long lastoffset = 0;
        boolean isRunning = true;
        ConsumerRecords<String, String> records=null;
        TopicPartition partition = new TopicPartition("cpu", 0);
        OffsetAndMetadata offsetAndMetadata=consumer.committed(partition);
        consumer.assign(Arrays.asList(partition));
        if(offsetAndMetadata!=null)
            consumer.seek(partition,offsetAndMetadata.offset());
        else {
            consumer.seek(partition,201);
        }
        while (isRunning) {
            records = consumer.poll(1000);
            System.out.println(records.count());
            if(records.count()!=0) {
                List<String> recordList = new ArrayList<String>();
                for (ConsumerRecord<String, String> record : records) {
                    lastoffset = record.offset();
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    log.info("offset = [{}], key = [{}], value =[{}]", record.offset(), record.key(), record.value());
                    System.out.println();
                    System.out.println(record.toString());
                    recordList.add(record.value());
                }
                try {
                    soffset = InsertToInfluxdb(recordList);
                    System.out.println("soffset-----:" + soffset);
                    System.out.println("lastoffset-----:" + lastoffset);
                    if (soffset == 0) {
                        System.out.println("成功插入influxdb");
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastoffset + 1)));
                    }
                }
                catch(Exception e){
                    if( soffset==-1 && e.getMessage().equals("influxDB批量写入失败"))
                        consumer.seek(partition, offsetAndMetadata.offset());
                    else
                        e.printStackTrace();
                }
            }
        }
    }

    public static void connect() throws SQLException {
        influxDB = InfluxDBFactory.connect("http://10.19.156.37:8086");
        Pong pong = influxDB.ping();
        if(pong != null){
            System.out.println("pong:" + pong + ",influxDB数据库连接成功");
        }else{
            System.out.println("influxDB数据库连接失败");
            throw new SQLException("influxDB数据库连接失败");
        }
    }

    public int InsertToInfluxdb(List<String> records) throws Exception {
        int flag = 0;
        JSONObject record = JSONObject.parseObject(records.get(0));
        batchPoints = BatchPoints.database(dbname)
                .tag("host", (String) record.get("host"))
                .tag("region", (String) record.get("region")).build();
        for (String content : records) {
            JSONObject json = JSONObject.parseObject(content);
            BigDecimal t = new BigDecimal(json.get("time").toString());
            BigDecimal time = t.multiply(new BigDecimal(1000));
            String timestamp = time.toString().split("\\.")[0];
            long tt = Long.parseLong(timestamp);
            Point point1 = Point.measurement("cpu")
                    .time(TimeUnit.NANOSECONDS.toNanos(tt), TimeUnit.NANOSECONDS)
                    .addField("load", Float.parseFloat(json.get("load").toString()))
                    .build();
            batchPoints.point(point1);
        }
        log.info("本次批量添加的记录有[{}]条",batchPoints.getPoints().size());
        QueryResult rs = null;
        try {
            influxDB.write(batchPoints);
        } catch (Exception e) {
            flag = -1;
            e.printStackTrace();
            throw new Exception("influxDB批量写入失败");
        }
        batchPoints = null;
        rs = query("select count(*) from cpu");
        System.out.println("result:" + rs.toString());
        return flag;
    }

    public QueryResult query(String command) {
        Query query = new Query(command, dbname);
        QueryResult result = influxDB.query(query);
        return result;
    }

    public static void main(String[] args){
        try {
            new KConsumer().consume();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
