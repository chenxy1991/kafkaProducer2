package com.thread2.kafkaStream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class AvgProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore kvStore;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.kvStore= (KeyValueStore) context.getStateStore("counts6");
        System.out.println("开始调用AvgProcessor。。。");
    }

    @Override
    public void process(String s, String s2) {
        System.out.println(s+","+s2);
        BigDecimal count = new BigDecimal(this.kvStore.get(s).toString());
        BigDecimal totalValue = new BigDecimal(s2);
        BigDecimal avg = totalValue.divide(count,2) ;
        System.out.println("聚合指标为:"+ s +",记录数为:"+count.toString()+",聚合的值为:"+ s2 + ",平均值为:"+avg.toString());
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            context.forward(entry.key, entry.value.toString());
        }
        iter.close();
        context.commit();      //提交当前处理进度
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
}
