package com.thread2.Consumer;

import java.io.InputStream;
import java.util.Properties;

public class Utils {

    public static Properties getProperties(String filename){
        String path="/"+filename;
        Properties props = new Properties();
        try {
            InputStream in = Utils.class.getResourceAsStream(path);
            props.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return props;
    }
}
