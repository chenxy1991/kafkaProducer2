package com.thread2.ConsumerThread;

import java.sql.SQLException;

public class ConsumerMain {

    public static void main(String[] args){

        /*final ConsumerGen consume=new ConsumerGen("cpu");
        consume.start(2);*/


        final testThread thread=new testThread();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run()
            {
                System.out.println("Execute Hook.....");
                thread.stop();
            }
        }));
        thread.start(2);







    }
}
