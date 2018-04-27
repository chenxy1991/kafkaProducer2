package com.thread2.kafkaStream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import  com.thread2.Consumer.Utils;

public class MyProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore kvStore;
    private KeyValueStore kcStore;
    int count=1;


    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(10000);
        this.kvStore = (KeyValueStore) context.getStateStore("counts5");
        this.kcStore = (KeyValueStore) context.getStateStore("counts6");
        System.out.println("开始调用MyProcessor。。。。");

    }

    //s为key，s2为value
    @Override
    public void process(String s, String s2) {

        System.out.println("这个MyProcessor对应处理"+context.topic()+"的第"+context.partition()+"的offset为"+context.offset()+"的记录");
        //获取聚合指标
        System.out.println("这是第"+(count++) +"条传输过来的记录，"+"key为:"+s+",value为:"+s2);
        String key=Utils.getKey(s);
        String value=s2.split("\\[")[1].split(",")[1].split("\"")[1];
        System.out.println("现在需要聚合的是:"+key+",value:"+value);
        String v= (String) this.kvStore.get(key);
        if(v == null){
            System.out.println("本地状态库中不存在对应的聚合指标:"+ key +","+"v:"+ v );
            this.kvStore.put(key,value);
            this.kcStore.put(key,1);
        }else{
            System.out.println("本地状态库中已有"+ key +",当前值v为："+v+",当前已有的记录数为："+this.kcStore.get(key).toString());
            BigDecimal bd1=new BigDecimal(v);
            BigDecimal bd2=new BigDecimal(value);
            this.kvStore.put(key,(bd1.add(bd2)).toString());
            this.kcStore.put(key,Integer.parseInt(this.kcStore.get(key).toString())+1);
            System.out.println("聚合指标为:"+key+",处理后的v值为:"+ (bd1.add(bd2)).toString());
            System.out.println("处理后的记录数为："+this.kcStore.get(key).toString());
        }
    }

    @Override
    public void punctuate(long l) {
        System.out.println("调用该方法的时间："+ l);
        int count = 1;
        KeyValueIterator iter = this.kvStore.all();
        while (iter.hasNext()) {
            KeyValue entry = (KeyValue) iter.next();
            context.forward(entry.key, entry.value.toString(),"Process2");
            System.out.println("punctuate的记录数是"+(count++));
        }
        iter.close();
        context.commit();      //提交当前处理进度
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
}
