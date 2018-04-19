package com.thread2.Consumer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DBOperation {

    public static BatchPoints batchPoints = null;
    private InfluxDB influxDB = null;
    public static String dbName = "cxy";
    private Logger log = LoggerFactory.getLogger("ConsumerLog");

    //单例模式获取influxDB连接
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

    //插入influxDB
    /*public synchronized boolean InsertToInfluxDB(List<String> records) throws Exception {
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
            batchPoints.point(point1);                       //批量加入batchPoints
        }
        try {
            influxDB.write(batchPoints);                     //写入influxdb
            isDone=true;
        } catch (Exception e) {
            e.printStackTrace();
            isDone=false;
        }
        log.info("本次批量添加的记录有[{}]条", batchPoints.getPoints().size());
        batchPoints = null;
        return isDone;
    }*/

    public synchronized boolean InsertToInfluxDB(List<String> records) throws Exception {
        Boolean isDone = false;
        batchPoints=ConstructBatchPoints(batchPoints,dbName,records.get(0));
        for (String content : records) {
            int index = content.indexOf("[");
            String time=content.substring(index+1, content.length() - 1).split(",")[0];
            String metricValue= content.substring(index+1, content.length() - 1).split(",")[1];
            long tt = transform(time);
            Point point1 = Point.measurement("cput")
                    .time(TimeUnit.NANOSECONDS.toNanos(tt), TimeUnit.NANOSECONDS)
                    .addField("value", metricValue)
                    .build();
            batchPoints.point(point1);
            System.out.println(point1.toString());
        }
        try {
            influxDB.write(batchPoints);                     //写入influxdb
            isDone=true;
        } catch (Exception e) {
            e.printStackTrace();
            isDone=false;
        }
        log.info("本次批量添加的记录有[{}]条", batchPoints.getPoints().size());
        batchPoints = null;
        return isDone;
    }

    public BatchPoints ConstructBatchPoints(BatchPoints batchPoints,String dbName,String record){
        int index=record.indexOf("[");
        Map<String,Object> map=new HashMap<String,Object>();
        String[] metricArray=record.substring(0,index-1).split(",");
        for(int i=0;i<metricArray.length;i++){
            System.out.println(metricArray[i]);
            String tagName=metricArray[i].split("=")[0];
            String tagValue=metricArray[i].split("=")[1];
            System.out.println(tagName+":"+tagValue);
            map.put(tagName,tagValue);
        }
        batchPoints = BatchPoints.database(dbName)
                .tag("cluster", map.get("cluster").toString())
                .tag("instance",map.get("instance").toString())
                .tag("host",map.get("host").toString())
                .tag("proj",map.get("proj").toString())
                .tag("job",map.get("job").toString())
                .tag("m",map.get("m").toString()).build();
       return batchPoints;
    }

    public QueryResult query(String command) {                    //查询操作
        Query query = new Query(command, dbName);
        QueryResult result = influxDB.query(query);
        return result;
    }

    public long transform(String content) {                            //将时间转换为long类型
        BigDecimal t = new BigDecimal(content);
        BigDecimal time = t.multiply(new BigDecimal(1000));
        String timestamp = time.toString().split("\\.")[0];
        long tt = Long.parseLong(timestamp);
        return tt;
    }

    //获取influxdb连接，设置失败重试次数，若超过次数还连接不上，则关闭influxdb连接
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

    //获取influxdb连接
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

    //获取influxdb url
    public String getInfluxDBUrl(String propsFile) {
        Properties props = Utils.getProperties(propsFile);
        return props.get("url").toString();
    }
}
