package com.thread2.kafkaStream;

import com.thread2.Utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class AvgProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore avgStore;
    private Logger log = LoggerFactory.getLogger("kafkaStreamLog");

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.avgStore = (KeyValueStore) context.getStateStore("avgStore");
        log.info("开始调用AvgProcessor。。。");
    }

    @Override
    public void process(String s, String s2) {
        log.info("这个AvgProcessor对应处理topic为[{}]的partition[{}]的offset为[{}]的记录",context.topic(),context.partition(),context.offset());
        String key=Utils.getKey(s);
        String value=s2.split("\\[")[1].split(",")[1].split("\"")[1];
        String v= (String)this.avgStore.get(key);
        if(v == null){
            this.avgStore.put(key,value);
        }else{
            BigDecimal oldValue = new BigDecimal(this.avgStore.get(key).toString());
            BigDecimal newValue = new BigDecimal(value);
            BigDecimal avg = oldValue.add(newValue).divide(new BigDecimal(2),2) ;
            this.avgStore.put(key,avg.toString());
        }

    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator iter = this.avgStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            log.info("聚合的key为[{}],平均值为[{}]",entry.key,entry.value);
            context.forward(entry.key,entry.key+",聚合平均值为:"+entry.value.toString());
            Utils.writeBackInfluxdb(context,entry,"avg");
        }
        iter.close();
        context.commit();      //提交当前处理进度
    }

    @Override
    public void close() {
        this.avgStore.close();
    }
}
