package com.thread2.kafkaStream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MyProcessor implements Processor<String, String> {
    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public void process(String s, String s2) {
        System.out.println(s+"-----"+s2);
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
