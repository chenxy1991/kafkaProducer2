package com.thread2.Producer;

import com.thread2.start.Start;
import junit.framework.TestCase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

public class KProducerTest extends TestCase {

    private Producer<String, String> producer;

    @Test
    public void testSetUp(){
        Properties props = new Properties();
        try {
            InputStream in = KProducer.class.getResourceAsStream("/producer.properties");
            props.load(in);
        }catch(Exception e){
            e.printStackTrace();
        }
        producer = new KafkaProducer<String, String>(props);
        System.out.println(producer);
    }
   /* @Test
    public void testProduce() {
        producer.
    }
    @Test
    public void produce(String s){

    }*/
   @Test
   public static void main(String[] args){

   }
}