package com.thread2.kafkaStream;


import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaStream {

    private static Logger log = LoggerFactory.getLogger("kafkaStreamLog");

    public static void main(String[] args){

        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-processing-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.19.156.37:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        StreamsConfig config = new StreamsConfig(props);

        KStreamBuilder builder = new KStreamBuilder();
        builder.addSource("source","cput")
                .addProcessor("countProcessor",CountProcessor::new,"source")
                .addProcessor("sumProcessor",SumProcessor::new,"source")
                .addProcessor("avgProcessor",AvgProcessor::new,"source")
                .addProcessor("medianProcessor",MedianProcessor::new,"source")
                .addProcessor("spreadProcessor", SpreadProcessor::new,"source")
                .addStateStore(Stores.create("countStore").withStringKeys().withStringValues().inMemory().build(), "countProcessor")
                .addStateStore(Stores.create("sumStore").withStringKeys().withStringValues().inMemory().build(), "sumProcessor")
                .addStateStore(Stores.create("avgStore").withStringKeys().withStringValues().inMemory().build(), "avgProcessor")
                .addStateStore(Stores.create("medianStore").withStringKeys().withStringValues().inMemory().build(), "medianProcessor")
                .addStateStore(Stores.create("minStore").withStringKeys().withStringValues().inMemory().build(), "spreadProcessor")
                .addStateStore(Stores.create("maxStore").withStringKeys().withStringValues().inMemory().build(), "spreadProcessor")
                .addSink("sink1","sum","sumProcessor")
                .addSink("sink2","count","countProcessor")
                .addSink("sink3","avg","avgProcessor")
                .addSink("sink4","spread","spreadProcessor");

       KafkaStreams streams = new KafkaStreams(builder, config);
       streams.start();
       log.info("启动kafkaStream....");
    }
}
