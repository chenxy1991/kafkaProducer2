package com.thread2.Producer;

import com.alibaba.fastjson.JSONObject;
import com.thread2.Consumer.Utils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class KProducer {

    private final Producer<String, String> producer;
    private Logger log = LoggerFactory.getLogger("ProducerLog");

    public static int i=1;

    public KProducer() {
        producer = new KafkaProducer<String, String>(Utils.getProperties("producer.properties"));
    }

    public void produce(String s){
            JSONObject content=JSONObject.parseObject(s);
            String topic = (String) content.get("table");
            long timeStamp = new BigDecimal(content.get("time").toString()).multiply(new BigDecimal(1000)).longValue();
            String key = timeStamp+"_"+"host="+content.get("host")+",region="+content.get("region");
            System.out.println(String.format("key = %s, value = %s",key,s));
           /* 发送key为timestamp+tags的record*/
           /* producer.send(new ProducerRecord<String, String>("cputest",key,s),new Callback() {
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
            });*/
           /*采用消息编号作为key*/
            System.out.println("生产者发送的第"+i+"条消息是"+s);
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
            });
        }
    }
