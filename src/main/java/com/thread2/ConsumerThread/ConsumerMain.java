package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class ConsumerMain {

    //private static Logger log = LoggerFactory.getLogger(ConsumerMain.class);
    private static Logger log = LoggerFactory.getLogger("ConsumerLog");

    public static void main(String[] args) {

        final Consumer<String, String> consumer;
        Properties props =getConsumerProperties();
        consumer = new KafkaConsumer<String, String>(props);

        final InfluxDB influxDB = DBOperation.getInstance().getInfluxDB();

        final ConsumerGen consume = new ConsumerGen("cpu", influxDB, consumer);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Execute Hook.....");
                log.info("Execute Hook....");
                consume.stop();
                consumer.close();
                influxDB.close();
            }
        }));
        consume.start(2);


       /* final testThread thread=new testThread();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run()
            {
                System.out.println("Execute Hook.....");
                thread.stop();
            }
        }));
        thread.start(2);*/
    }

    public static Properties getConsumerProperties() {
        Properties props = new Properties();
        try {
            InputStream in = ConsumerMain.class.getResourceAsStream("/consumer.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }
}
