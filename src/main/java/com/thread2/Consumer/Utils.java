package com.thread2.Consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.awt.*;
import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

//工具类，包括读配置文件和写文件
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

    public static long readFromFile(TopicPartition partition, String filename) {
        BufferedReader br = null;
        String str = null;
        long offset = 0L;
        File file = new File(Offset.class.getResource("/"+filename).getPath());
        try {
            br = new BufferedReader(new FileReader(file));
            while ((str = br.readLine()) != null) // 判断最后一行不存在，为空结束循环
            {
                if (String.valueOf(partition.partition()).equals(str.split(":")[0])) {
                    offset = Long.parseLong(str.split(":")[1]);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }

    //写入文件的内容格式为 partitionNum:offset
    public static void saveToFile(Map<TopicPartition,Map<TopicPartition, OffsetAndMetadata>> commitMaps, String filename) {
        BufferedWriter Buff = null;
        File file = new File(Offset.class.getResource("/"+filename).getPath());
        try {
            Buff = new BufferedWriter(new FileWriter(file, false));
            for(TopicPartition partition : commitMaps.keySet() ) {
                    System.out.println("开始写入文件.....");
                    Buff.write(partition.partition() + ":" + String.valueOf(commitMaps.get(partition).get(partition).offset()));
                    Buff.write("\n");
            }
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
