package com.thread2.ConsumerThread;

import java.io.InputStream;
import java.util.Properties;

public class Utils {

    public static Properties getConsumerProperties() {
        Properties props = new Properties();
        try {
            InputStream in = Utils.class.getResourceAsStream("/consumer.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }

    public static Properties getProducerProperties() {
        Properties props = new Properties();
        try {
            InputStream in = Utils.class.getResourceAsStream("/producer.properties");
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }
}
