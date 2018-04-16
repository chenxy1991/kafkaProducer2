package com.thread2.kafkaStream;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaStream {

    public static void main(String[] args){

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.19.156.37:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        /*builder.addSource("source","cputest").addProcessor("Processer1",MyProcessor::new,"source")
        .addSink("sink1","cputest","Processer1");*/
        builder.stream("cputest").map((key,value)->new KeyValue<>(value,key))
                .flatMapValues(key->Arrays.asList(key.toString().split("_")[1]))
                .map((key,value)-> new KeyValue<>(value,key))
                .print();
        //"host="+JSONObject.parseObject(value.toString()).get("host")+",region="+JSONObject.parseObject(value.toString()).get("region")))
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
