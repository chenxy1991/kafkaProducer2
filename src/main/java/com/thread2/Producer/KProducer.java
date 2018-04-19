package com.thread2.Producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.thread2.Consumer.Utils;
import com.thread2.start.Metric;
import com.thread2.start.MetricUnit;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;

public class KProducer {

    private final Producer<String, String> producer;
    private Logger log = LoggerFactory.getLogger("ProducerLog");   //打印生产者日志

    public static int i = 1;

    //在构造函数中读取配置文件生成生产者实例
    public KProducer() {
        producer = new KafkaProducer<String, String>(Utils.getProperties("producer.properties"));
    }

    /* public void produce(String s){
             JSONObject content=JSONObject.parseObject(s);    //将String类型的消息转换为json格式
             String topic = (String) content.get("table");    //获取topic的名字
             long timeStamp = new BigDecimal(content.get("time").toString()).multiply(new BigDecimal(1000)).longValue(); //将时间转换为long型
             String key = timeStamp+"_"+"host="+content.get("host")+",region="+content.get("region");   //构造消息的key值，key为timestamp+tags(host,region)
             System.out.println(String.format("key = %s, value = %s",key,s));*/
    /* 调用producer的send方法发送key为timestamp+tags的record，s为消息*/
          /*  producer.send(new ProducerRecord<String, String>("cputest",key,s),new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                        e.printStackTrace();
                        log.info(e.getMessage());
                    } else {
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        log.info("The offset of the record we just sent is:[{}]",metadata.offset());
                        System.out.println(String.format("partition = %s, offset = %d", metadata.partition(), metadata.offset()));
                    }
                }
            });
           /*采用消息编号作为key，调用producer的send方法*/
           /* System.out.println("生产者发送的第"+i+"条消息是"+s);
            log.info("生产者发送的第[{}]条消息是[{}]",i,s);
            producer.send(new ProducerRecord<String, String>("cpu", 0,Integer.toString(i++), s),new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                        e.printStackTrace();
                        log.info(e.getMessage());
                    } else {
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        log.info("The offset of the record we just sent is:[{}]",metadata.offset());
                    }
                }
            });*/
    public void produce(List<MetricUnit> s) {
        for (MetricUnit message : s) {
            System.out.println(message.toString());
            String timestamp = message.getValue().split(",")[0].replace("[","");
            String tags = message.getTags();
            long time = new BigDecimal(timestamp).multiply(new BigDecimal(1000)).longValue();
            String key=time+"_"+tags;
            System.out.println(key);
            /*String key = message.getValue().split(",")[0].replace("[","");
                    +"_"+"host="+content.get("host")+",region="+content.get("region");
                String topic = (String) content.get("table");    //获取topic的名字
                long timeStamp = new BigDecimal(content.get("time").toString()).multiply(new BigDecimal(1000)).longValue(); //将时间转换为long型
                String key = timeStamp+"_"+"host="+content.get("host")+",region="+content.get("region");   //构造消息的key值，key为timestamp+tags(host,region)
                System.out.println(String.format("key = %s, value = %s",key,s));*/
                //调用producer的send方法发送key为timestamp+tags的record，s为消息
                producer.send(new ProducerRecord<String, String>("cput",key,message.toString()),new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if(e != null) {
                        e.printStackTrace();
                        log.info(e.getMessage());
                    } else {
                        System.out.println("The offset of the record we just sent is: " + metadata.offset());
                        log.info("The offset of the record we just sent is:[{}]",metadata.offset());
                        System.out.println(String.format("partition = %s, offset = %d", metadata.partition(), metadata.offset()));
                    }
                }
            });
        }
    }
}