package com.thread2.Consumer;

import com.alibaba.fastjson.JSONObject;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DBOperation {

    public static BatchPoints batchPoints = null;
    private InfluxDB influxDB = null;
    public static String dbName = "cxy";
    private Logger log = LoggerFactory.getLogger("ConsumerLog");

    private DBOperation() {
        influxDB = connectDB(3,"db.properties");
    }

    private static class DBOperationHolder {
        private static DBOperation operation = new DBOperation();
    }

    public static DBOperation getInstance() {
        return DBOperationHolder.operation;
    }

    public InfluxDB getInfluxDB() {
        return influxDB;
    }

    public synchronized boolean InsertToInfluxDB(List<String> records) throws Exception {
        Boolean isDone = false;
        JSONObject record = JSONObject.parseObject(records.get(0));
        batchPoints = BatchPoints.database(dbName)
                .tag("host", (String) record.get("host"))
                .tag("region", (String) record.get("region")).build();
        for (String content : records) {
            System.out.println("插入的数据为："+content);
            JSONObject json = JSONObject.parseObject(content);
            long tt = transform(content);
            Point point1 = Point.measurement("cpu")
                    .time(TimeUnit.NANOSECONDS.toNanos(tt), TimeUnit.NANOSECONDS)
                    .addField("load", Float.parseFloat(json.get("load").toString()))
                    .build();
            batchPoints.point(point1);
        }
        try {
            influxDB.write(batchPoints);
            isDone=true;
        } catch (Exception e) {
            e.printStackTrace();
            isDone=false;
        }
        log.info("本次批量添加的记录有[{}]条", batchPoints.getPoints().size());
        batchPoints = null;
        return isDone;
    }

    public QueryResult query(String command) {
        Query query = new Query(command, dbName);
        QueryResult result = influxDB.query(query);
        return result;
    }

    public long transform(String content) {
        JSONObject json = JSONObject.parseObject(content);
        BigDecimal t = new BigDecimal(json.get("time").toString());
        BigDecimal time = t.multiply(new BigDecimal(1000));
        String timestamp = time.toString().split("\\.")[0];
        long tt = Long.parseLong(timestamp);
        return tt;
    }

    private InfluxDB connectDB(int times,String propsFile) {
        try {
            influxDB = connect(propsFile);
        } catch (SQLException e) {
            if (e.getMessage().equals("influxDB数据库连接失败")) {
                int time = 1;
                while (time <= times) {
                    try {
                        connect(propsFile);
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

    private InfluxDB connect(String propsFile) throws SQLException {
        String url = getInfluxDBUrl(propsFile);
        influxDB = InfluxDBFactory.connect(url);
        Pong pong = influxDB.ping();
        if (pong != null) {
            System.out.println("pong:" + pong + ",influxDB数据库连接成功");
            log.info("[{}],influxDB数据库连接成功", pong);
        } else {
            System.out.println("influxDB数据库连接失败");
            log.info("influxDB数据库连接失败");
            throw new SQLException("influxDB数据库连接失败");
        }
        return influxDB;
    }

    public String getInfluxDBUrl(String propsFile) {
        Properties props = Utils.getProperties(propsFile);
        return props.get("url").toString();
    }
}
