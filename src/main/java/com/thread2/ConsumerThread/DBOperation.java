package com.thread2.ConsumerThread;

import com.alibaba.fastjson.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DBOperation {

    private static DBOperation operation=null;
    public static BatchPoints batchPoints = null;
    private InfluxDB influxDB = null;
    public static String dbname = "cxy";
    private static Logger log = LoggerFactory.getLogger(DBOperation.class);

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

    public synchronized long InsertToInfluxdb(List<String> records) throws Exception {
       // long flag = 0;
        JSONObject record = JSONObject.parseObject(records.get(0).split(",")[0]);
        long initoffset=Long.valueOf(records.get(0).split(",")[1]);
        long lastoffset=0L;
        batchPoints = BatchPoints.database(dbname)
                      .tag("host", (String) record.get("host"))
                      .tag("region", (String) record.get("region")).build();
        for (String ValueAndOffset : records) {
            System.out.println("-----" + Thread.currentThread().getName()+":"+"获取到的数据库连接："+influxDB);
            String content=ValueAndOffset.split(",")[0];
            lastoffset=Long.valueOf(ValueAndOffset.split(",")[1]);
            JSONObject json = JSONObject.parseObject(content);
            long tt=transform(content);
            Point point1 = Point.measurement("cpu")
                       .time(TimeUnit.NANOSECONDS.toNanos(tt), TimeUnit.NANOSECONDS)
                       .addField("load", Float.parseFloat(json.get("load").toString()))
                       .build();
            batchPoints.point(point1);
        }
        QueryResult rs = null;
        try {
            influxDB.write(batchPoints);
           // flag=lastoffset;
        } catch (Exception e) {
           // flag = -1L;
            lastoffset=-1L;
            e.printStackTrace();
            throw new Exception("influxDB批量写入失败");
        }
        log.info("本次批量添加的记录有[{}]条", batchPoints.getPoints().size());
        batchPoints = null;
        rs = query("select count(*) from cpu");
        System.out.println("result:" + rs.toString());
        return lastoffset;
    }

    public QueryResult query(String command) {
        Query query = new Query(command, dbname);
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
        influxDB = InfluxDBFactory.connect("http://10.19.156.37:8086");
        Pong pong = influxDB.ping();
        if (pong != null) {
            System.out.println("pong:" + pong + ",influxDB数据库连接成功");
        } else {
            System.out.println("influxDB数据库连接失败");
            throw new SQLException("influxDB数据库连接失败");
        }
        return influxDB;
    }
}
