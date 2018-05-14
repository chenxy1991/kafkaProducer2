package com.thread2.Producer;

import com.thread2.Utils.Utils;
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

    public void produce(List<MetricUnit> s) {
        for (MetricUnit message : s) {
            log.info("要发送的message为[{}]",message.toString());
            String timestamp = message.getValue().split(",")[0].replace("[", "");
            String tags = message.getTags();
            String topic = message.getMetricName();
            long time = new BigDecimal(timestamp).multiply(new BigDecimal(1000)).longValue();
            String key = time + "_" + tags;
            log.info("message的key=[{}],topic=[{}]",key,topic);

            //调用producer的send方法发送key为timestamp+tags的record，s为消息
            producer.send(new ProducerRecord<String, String>("cput", key, message.toString()), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        log.info("The offset of the record we just sent is:[{}],the partition of the record is partition[{}]", metadata.offset(),metadata.partition());
                    }
                }
            });
        }
    }
}