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

public class SumProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore kvStore;
    private Logger log = LoggerFactory.getLogger("kafkaStreamLog");

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.kvStore = (KeyValueStore) context.getStateStore("sumStore");
        log.info("开始调用SumProcessor。。。。");
    }

    //s为key，s2为value
    @Override
    public void process(String s, String s2) {
        log.info("这个SumProcessor对应处理topic为[{}]的partition[{}]的offset为[{}]的记录",context.topic(),context.partition(),context.offset());

        //获取聚合指标
        String key=Utils.getKey(s);
        String value=s2.split("\\[")[1].split(",")[1].split("\"")[1];
        log.info("现在SumProcessor需要聚合的key是[{}],value是[{}]",key,value);

        String v= (String) this.kvStore.get(key);
        if(v == null){
            log.info("本地状态库中不存在对应的聚合指标 key[{}],value[{}]",key,v);
            this.kvStore.put(key,value);
        }else{
            log.info("本地状态库中已有 key[{}],value[{}]",key,v);
            BigDecimal bd1=new BigDecimal(v);
            BigDecimal bd2=new BigDecimal(value);
            this.kvStore.put(key,(bd1.add(bd2)).toString());
            log.info("聚合指标为[{}],处理后的v值为[{}]",key,(bd1.add(bd2)).toString());
        }
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            context.forward(entry.key, entry.key+",聚合的sum为:"+entry.value.toString());
            Utils.writeBackInfluxdb(context,entry,"sum");
        }
        iter.close();
        context.commit();      //提交当前处理进度
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
}
