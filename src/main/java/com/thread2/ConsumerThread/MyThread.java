package com.thread2.ConsumerThread;

import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MyThread implements Runnable {

    private ThreadLocal<Integer> i = new ThreadLocal<>();

    @Override
    public void run() {
        i.set(0);

      /*  for(int i=0;i<5;i++){
            //System.out.println(Thread.currentThread().getName()+"-------"+DBOperation.getInstance().hashCode());
            //System.out.println(Thread.currentThread().getName()+"-------"+DBOperation.getInstance().getInfluxDB().hashCode());
            //System.out.println(Thread.currentThread().getName()+"-------"+i);
            try {
                if (i == 3) {
                    int a = 1 / 0;
                }
            System.out.println(Thread.currentThread().getName()+"-------"+i);
            }catch(ArithmeticException e){
                e.printStackTrace();
            }
        }
        System.out.println("come on");*/
        try {
            TimeUnit.SECONDS.sleep((int) Math.rint(Math.random() * 10));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (i.get() <= 5) {
            System.out.println(Thread.currentThread().getName() + "-------" + i.get());
            i.set(i.get() + 1);
        }
    }

    public static void main(String[] args) {
      /*  MyThread[] mts=new MyThread[3];
        for(int i=0;i<mts.length;i++){
            mts[i]=new MyThread();
        }
        for(int j=0;j<mts.length;j++){
            Thread thread =new Thread(mts[j]);
            thread.start();
        }
        MyThread thread=new MyThread();
        if(thread != null) {
            System.out.println("cxy");
        }
    }
        /*MyThread thread = new MyThread();
        for (int i = 0; i <= 3; i++) {
            Thread thread1 = new Thread(thread);
            thread1.start();
        }*/

       /* List<String> commitQueue = new ArrayList<String>();
        commitQueue.add("cxy");
        commitQueue.add("lsj");
        commitQueue.add("gyt");

        LinkedBlockingQueue<String> offsetQueue =new LinkedBlockingQueue<>();

        offsetQueue.addAll(commitQueue);

        System.out.println(offsetQueue.toString());*/

        /*long finalOffset= readFromFile("/offset.txt");
        System.out.println(finalOffset);*/
        saveToFile(1000,"/offset.txt");
        long finalOffset= readFromFile("/offset.txt");
        System.out.println(finalOffset);
    }

    public static void saveToFile(long offset,String filename)  {
        BufferedWriter Buff = null;
        File file = new File(MyThread.class.getResource(filename).getPath());
        try {
            Buff =new BufferedWriter(new FileWriter(file));
            Buff.write(String.valueOf(offset));
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long readFromFile(String filename){
       /* File file = new File(MyThread.class.getResource(filename).getPath());
        System.out.println(MyThread.class.getResource(filename).getPath());
        FileInputStream in = null;
        long offset=0L;
        try {
            in = new FileInputStream(file);
            offset = (long)in.read();
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;*/
        BufferedReader bre = null;
        String str=null;
        long offset=0L;
        File file = new File(MyThread.class.getResource(filename).getPath());
        try {
            bre = new BufferedReader(new FileReader(file));
            while ((str = bre.readLine())!= null) // 判断最后一行不存在，为空结束循环
            {
               offset=Long.parseLong(str);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }
}

