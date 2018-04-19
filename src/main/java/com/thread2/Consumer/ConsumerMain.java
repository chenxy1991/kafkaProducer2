package com.thread2.Consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.InfluxDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//consumer启动类
public class ConsumerMain {

    private static Logger log = LoggerFactory.getLogger("ConsumerLog");
    //创建consumer，获取influxdb连接
    private static final Consumer<String, String> consumer = new KafkaConsumer<String, String>(Utils.getProperties("consumer.properties"));
    private static final InfluxDB influxDB = DBOperation.getInstance().getInfluxDB();
    private static final ConsumerGen consume = new ConsumerGen("cput", influxDB, consumer);


    public static void main(String[] args) {

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

        //要启动两个worker线程
        consume.start(2);
    }


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
