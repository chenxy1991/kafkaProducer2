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


public class SpreadProcessor implements Processor<String, String> {

    private ProcessorContext context;
    private KeyValueStore minStore;
    private KeyValueStore maxStore;
    private Logger log = LoggerFactory.getLogger("kafkaStreamLog");


    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.context.schedule(1000);
        this.minStore = (KeyValueStore) context.getStateStore("minStore");
        this.maxStore = (KeyValueStore) context.getStateStore("maxStore");
        System.out.println("开始调用SpreadProcessor。。。。");
        log.info("开始调用SpreadProcessor。。。。");
    }

    @Override
    public void process(String s, String s2) {

        System.out.println("这个SpreadProcessor对应处理"+context.topic()+"的第"+context.partition()+"的offset为"+context.offset()+"的记录");
        log.info("这个SpreadProcessor对应处理topic为[{}]的partition为[{}]的offset为[{}]的记录",context.topic(),context.partition(),context.offset());

        String key=Utils.getKey(s);
        String value=s2.split("\\[")[1].split(",")[1].split("\"")[1];
        String vmin= (String)this.minStore.get(key);
        String vmax= (String)this.maxStore.get(key);
        if(vmin == null){
            this.minStore.put(key,value);
        }
        else{
            BigDecimal oldValue = new BigDecimal((String)this.minStore.get(key));
            BigDecimal newValue = new BigDecimal(value);
            if(newValue.compareTo(oldValue) < 1){
                 this.minStore.put(key,newValue.toString());
            }else{
                if(vmax == null){
                    this.maxStore.put(key,newValue.toString());
                }
                else{
                   BigDecimal maxnum=new BigDecimal(vmax);
                   if(newValue.compareTo(maxnum) ==1){
                       this.maxStore.put(key,newValue.toString());
                   }
                }
            }
        }
    }
        @Override
        public void punctuate(long l) {
            KeyValueIterator iter = this.minStore.all();
            while (iter.hasNext()) {
                KeyValue entry = (KeyValue) iter.next();
                BigDecimal maxnum=new BigDecimal((String)this.maxStore.get(entry.key));
                BigDecimal minnum=new BigDecimal((String)entry.value);
                BigDecimal difference= maxnum.subtract(minnum);
                context.forward(entry.key,entry.key+",最大值和最小值之间的差值为:"+difference);
                KeyValue writeInDB = new KeyValue(entry.key,difference.toString());
                Utils.writeBackInfluxdb(context,writeInDB,"spread");
            }
            iter.close();
            context.commit();      //提交当前处理进度
        }

        @Override
        public void close() {
            this.minStore.close();
            this.maxStore.close();
        }
    }
