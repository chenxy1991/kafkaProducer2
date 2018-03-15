package com.thread2.ConsumerThread;

import java.util.concurrent.*;

public class testThread {

    private ExecutorService executor;
    final long awaitTime = 5 * 1000;
    public static boolean isRunning = true;


    public void start(int threadNum) {
        executor = new ThreadPoolExecutor(threadNum, threadNum, 2L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());
        Result result=new Result();
        while(isRunning) {
            try {
                Future<Result> future=executor.submit(new hellowordThread(result),result);
                System.out.println(future.get().isDoneFlag());
            }catch(ArithmeticException e){
                isRunning=false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
          // System.exit(0);
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
