package com.thread2.ConsumerThread;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.TopicPartition;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DBOperation {

    private static DBOperation operation=null;
    public static BatchPoints batchPoints = null;
    private InfluxDB influxDB = null;
    public static String dbName = "cxy";
    //private static Logger log = LoggerFactory.getLogger(DBOperation.class);
    private Logger log = LoggerFactory.getLogger("ConsumerLog");
    private DBOperation(){
        influxDB=connectDB(3);
    }

    private static class DBOperationHolder{
        private static DBOperation operation= new DBOperation();
    }

    public static DBOperation getInstance(){
        return DBOperationHolder.operation;
    }

    public InfluxDB getInfluxDB() {
        return influxDB;
    }

    public synchronized Offset InsertToInfluxdb(List<String> records) throws Exception {
        Offset result=null;
        System.out.println("-----" + Thread.currentThread().getName() + ":" + "获取到的数据库连接：" + influxDB);
        log.info(Thread.currentThread().getName() + ":" + "获取到的数据库连接[{}]",influxDB);
        JSONObject record = JSONObject.parseObject(records.get(0).split("&")[0]);
        long initoffset = Long.valueOf(records.get(0).split("&")[1]);
        long lastoffset = 0L;
        batchPoints = BatchPoints.database(dbName)
                .tag("host", (String) record.get("host"))
                .tag("region", (String) record.get("region")).build();
        for (String ValueAndOffset : records) {
            String content = ValueAndOffset.split("&")[0];
            System.out.println(Thread.currentThread().getName()+"记录是 :"+content);
            log.info(Thread.currentThread().getName()+"记录是[{}]",content);
            lastoffset = Long.valueOf(ValueAndOffset.split("&")[1]);
            System.out.println(Thread.currentThread().getName()+"记录的offset是 :"+lastoffset);
            log.info(Thread.currentThread().getName()+"记录的offset是[{}]",lastoffset);
            JSONObject json = JSONObject.parseObject(content);
            long tt = transform(content);
            Point point1 = Point.measurement("cpu")
                    .time(TimeUnit.NANOSECONDS.toNanos(tt), TimeUnit.NANOSECONDS)
                    .addField("load", Float.parseFloat(json.get("load").toString()))
                    .build();
            batchPoints.point(point1);
        }
        QueryResult rs = null;
        try {
            influxDB.write(batchPoints);
            result=new Offset();
            result.setInitOffset(initoffset);
            result.setLastOffset(lastoffset);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName()+"插入的该批记录的offset初始值为"+result.getInitOffset()+",最后一条记录的偏移值为"+result.getLastOffset());
        log.info(Thread.currentThread().getName()+"插入的该批记录的offset初始值为[{}],最后一条记录的偏移值为[{}]",result.getInitOffset(),result.getLastOffset());
        log.info("本次批量添加的记录有[{}]条", batchPoints.getPoints().size());
        batchPoints = null;
        return result;
    }

    public QueryResult query(String command) {
        Query query = new Query(command, dbName);
        QueryResult result = influxDB.query(query);
        return result;
    }

    public long transform(String content){
        JSONObject json = JSONObject.parseObject(content);
        BigDecimal t = new BigDecimal(json.get("time").toString());
        BigDecimal time = t.multiply(new BigDecimal(1000));
        String timestamp = time.toString().split("\\.")[0];
        long tt = Long.parseLong(timestamp);
        return tt;
    }

    private InfluxDB connectDB(int times) {
        try {
            influxDB=connect();
        } catch (SQLException e) {
            if (e.getMessage().equals("influxDB数据库连接失败")) {
                int time = 1;
                while (time <= times) {
                    try {
                        connect();
                    } catch (Exception es) {
                        es.printStackTrace();
                        time++;
                    }
                }
                influxDB.close();
            }
        }
        return influxDB;
    }

    private InfluxDB connect() throws SQLException {
        String url= getDBUrl();
        influxDB = InfluxDBFactory.connect(url);
        Pong pong = influxDB.ping();
        if (pong != null) {
            System.out.println("pong:" + pong + ",influxDB数据库连接成功");
            log.info("[{}],influxDB数据库连接成功",pong);
        } else {
            System.out.println("influxDB数据库连接失败");
            log.info("influxDB数据库连接失败");
            throw new SQLException("influxDB数据库连接失败");
        }
        return influxDB;
    }

    public String getDBUrl() {
        Properties props = new Properties();
        try {
            InputStream in = ConsumerMain.class.getResourceAsStream("/db.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props.get("url").toString();
    }
}
