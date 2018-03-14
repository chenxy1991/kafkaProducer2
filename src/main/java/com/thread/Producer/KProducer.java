package com.thread.Producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
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
