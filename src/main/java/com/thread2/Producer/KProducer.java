package com.thread2.Producer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Properties;

public class KProducer {

    private final Producer<String, String> producer;
    private static Logger log = LoggerFactory.getLogger(KProducer.class);

    public static int i=1;

    public KProducer() {
        Properties props = new Properties();
        try {
            InputStream in = KProducer.class.getResourceAsStream("/producer.properties");
            props.load(in);
        }catch(Exception e){
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, String>(props);
    }

    public void produce(String s){
            JSONObject content=JSONObject.parseObject(s);
            String topic = (String) content.get("table");
            long timeStamp = new BigDecimal(content.get("time").toString()).multiply(new BigDecimal(1000)).longValue();
            //String key = topic +"_"+ timeStamp+"_"+"host="+content.get("host")+",region="+content.get("region");
            String key = timeStamp+"_"+"host="+content.get("host")+",region="+content.get("region");
            System.out.println(String.format("key = %s, value = %s",key,s));
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
