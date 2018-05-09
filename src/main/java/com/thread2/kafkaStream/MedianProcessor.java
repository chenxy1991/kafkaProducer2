package com.thread2.kafkaStream;

import com.thread2.Utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MedianProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore medianStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.medianStore = (KeyValueStore) context.getStateStore("medianStore");
        System.out.println("开始调用MedianProcessor。。。");
    }

    @Override
    public void process(String s, String s2) {

    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
