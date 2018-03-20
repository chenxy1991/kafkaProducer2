package com.thread2.ConsumerThread;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class testThread {

    private ExecutorService executor;
    final long awaitTime = 5 * 1000;
    public static boolean isRunning = true;


    public void start(int threadNum) {
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        Result result=new Result();
        LinkedBlockingQueue offsetQueue = new LinkedBlockingQueue<List<String>>();
        while(isRunning) {
            try {
                List<String> offsets = new ArrayList<String>();
                offsets.add("cxy");
                executor.submit(new hellowordThread(offsets,offsetQueue));
                //System.out.println(future.get().isDoneFlag());
            }catch(ArithmeticException e){
                isRunning=false;
            }
           System.out.println("------"+offsetQueue.toString());
           System.out.println("i am ok");

        }
        System.exit(0);

    }

    public void stop(){
        System.out.println("testThread正在关闭。。。");
        try{
           executor.shutdown();
            if(!executor.awaitTermination(awaitTime, TimeUnit.MILLISECONDS)){
                // 超时的时候向线程池中所有的线程发出中断(interrupted)。
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            // awaitTermination方法被中断的时候也中止线程池中全部的线程的执行。
            System.out.println("awaitTermination interrupted: " + e);
            executor.shutdownNow();
        }
    }
}
