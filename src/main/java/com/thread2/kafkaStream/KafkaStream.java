package com.thread2.kafkaStream;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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
        builder.addSource("source","cput")
                .addProcessor("Processer1",MyProcessor::new,"source")
                .addProcessor("Process2",AvgProcessor::new,"Processer1")
                .addStateStore(Stores.create("counts5").withStringKeys().withStringValues().inMemory().build(), "Processer1")
                .addStateStore(Stores.create("counts6").withStringKeys().withIntegerValues().inMemory().build(), "Processer1")
                .connectProcessorAndStateStores("Process2", "counts6")
                .addSink("sink1","test","Process2");
       /* builder.stream("cputest").map((key,value)->new KeyValue<>(value,key))
                .flatMapValues(key->Arrays.asList(key.toString().split("_")[1]))
                .map((key,value)-> new KeyValue<>(value,key))
                .print();
        KStream source1 = builder.stream("cput");*/
      /*KTable<String,Double> count = source1.flatMapValues(value->Arrays.asList(value.toString().split("\\[")[1].split(",")[1].split("\"")[1]))
                .map((key,value)->new KeyValue<>(key,Double.parseDouble(value.toString().trim())))
                .groupByKey().count("us");
        count.toStream().print();*/

     /*source1.map(new KeyValueMapper<String,String,KeyValue<String,Double>>() {
        @Override
        public KeyValue<String, Double> apply(String key, String value) {
            int index=key.split("_")[1].indexOf("instance");
            String cluster = key.split("_")[1].substring(0,index-1);
            return new KeyValue<>(cluster,Double.parseDouble(value.toString().split("\\[")[1].split(",")[1].split("\"")[1]));
        }
    }).groupBy(new KeyValueMapper<String,Double,KeyValue<String,Double>>() {

         @Override
         public KeyValue<String, Double> apply(String s, Double aDouble) {
             aDouble = aDouble + aDouble;

             System.out.println("key is" + s +",value is" + aDouble);
             return new KeyValue<>(s,aDouble);
         }
     });*/

       KafkaStreams streams = new KafkaStreams(builder, config);
       streams.start();
    }
}
