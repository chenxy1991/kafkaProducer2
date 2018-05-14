package com.thread2.kafkaStream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import com.thread2.Utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore kcStore;
    private Logger log = LoggerFactory.getLogger("kafkaStreamLog");


    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.kcStore = (KeyValueStore) context.getStateStore("countStore");
        log.info("开始调用CountProcessor。。。。");
    }

    //s为key，s2为value
    @Override
    public void process(String s, String s2) {
        log.info("这个CountProcessor对应处理topic为[{}]的partition[{}]的offset为[{}]的记录",context.topic(),context.partition(),context.offset());
        //获取聚合指标
        String key=Utils.getKey(s);
        String v= (String) this.kcStore.get(key);
        if(v == null){
            log.info("本地状态库中不存在对应的聚合指标 key[{}],value[{}]",key,v);
            this.kcStore.put(key,"1");
        }else{
            log.info("本地状态库中已有key[{}],当前值v为[{}],当前已有的记录数为[{}]：",key,v,this.kcStore.get(key).toString());
            int count=Integer.parseInt(this.kcStore.get(key).toString())+1;
            this.kcStore.put(key,String.valueOf(count));
            log.info("处理后的记录数为[{}]",this.kcStore.get(key).toString());
        }
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator iter = this.kcStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            context.forward(entry.key, entry.key+",聚合的记录数为:"+entry.value.toString());
            Utils.writeBackInfluxdb(context,entry,"count");
        }
        iter.close();
        context.commit();      //提交当前处理进度
    }

    @Override
    public void close() {
        this.kcStore.close();
    }
}
