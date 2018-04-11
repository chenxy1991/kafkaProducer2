package com.thread2.Mytest;

import com.thread2.Consumer.ConsumerGen;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyThread implements Runnable {

    //private ThreadLocal<Integer> i = new ThreadLocal<>();
    AtomicBoolean isDone = new AtomicBoolean(false);

    @Override
    public void run() {
        isDone.set(true);

        if(isDone.get()){
            System.out.println(Thread.currentThread().getName()+"----cxy");
        }

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
       /* while (i.get() <= 5) {
            System.out.println(Thread.currentThread().getName() + "-------" + i.get());
            i.set(i.get() + 1);
        }*/
    }

    public static void main(String[] args) {
        Map<Integer,String> commitMap=new HashMap<>();
        for(Integer m:commitMap.keySet()){
            System.out.println("----"+m+":"+commitMap.get(m));
        }

       /* MyThread[] mts = new MyThread[3];
        for (int i = 0; i < mts.length; i++) {
            mts[i] = new MyThread();
        }
        for (int j = 0; j < mts.length; j++) {
            Thread thread = new Thread(mts[j]);
            thread.start();
        }*/
       /* MyThread thread = new MyThread();
        if (thread != null) {
            System.out.println("cxy");
        }*/
       // readFromFile(1,"/offset.txt");
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
        /*saveToFile(1000,"/offset.txt");
        long finalOffset= readFromFile("/offset.txt");
        System.out.println(finalOffset);*/

       /* Map<TopicPartition,Long> lastCommited=new HashMap<>();
        if(lastCommited.get(0) == null)
        {
            lastCommited.put(0,getLastCommited());
        }*/
       /* Map<Integer,String> commitMap=new HashMap<>();
        commitMap.put(1,"33");
        commitMap.put(2,"34");
        commitMap.put(3,"35");
        saveToFile(commitMap, "/offset.txt");
        readFromFile(1,"/offset.txt");
        readFromFile(2,"/offset.txt");
        readFromFile(3,"/offset.txt");*/

    public static void saveToFile(Map<Integer,String> commitMap, String filename)  {
        BufferedWriter Buff = null;
        File file = new File(MyThread.class.getResource(filename).getPath());
        try {
            Buff =new BufferedWriter(new FileWriter(file,false));
            for(Integer partition:commitMap.keySet()){
                Buff.write(partition+":"+String.valueOf(commitMap.get(partition)));
                Buff.write("\n");
            }
            Buff.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static long readFromFile(Integer partition,String filename){
        BufferedReader br = null;
        String str=null;
        long offset=0L;
        System.out.println(ConsumerGen.class.getResource(filename).getPath());
        File file = new File(ConsumerGen.class.getResource(filename).getPath());
        try {
            br = new BufferedReader(new FileReader(file));
            while ((str = br.readLine())!= null) // 判断最后一行不存在，为空结束循环
            {
                if(String.valueOf(partition).equals(str.split(":")[0])) {
                    offset = Long.parseLong(str.split(":")[1]);
                    System.out.println(offset);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return offset;
    }
}

