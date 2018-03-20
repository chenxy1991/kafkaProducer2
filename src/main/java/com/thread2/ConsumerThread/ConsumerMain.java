package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.InfluxDB;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

public class ConsumerMain {

    public static void main(String[] args) {

        final Consumer<String, String> consumer;
        Properties props = getConsumerProperties();
        consumer = new KafkaConsumer<String, String>(props);

        InfluxDB influxDB = DBOperation.getInstance().getInfluxDB();

        final ConsumerGen consume = new ConsumerGen("cpu", influxDB, consumer);
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Execute Hook.....");
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
        thread.start(2);
    }*/
    }

    public static Properties getConsumerProperties() {
        Properties props = new Properties();
        try {
            InputStream in = ConsumerGen.class.getResourceAsStream("/consumer.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }
}
