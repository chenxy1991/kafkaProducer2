package com.thread2.ConsumerThread;

public class MyThread extends Thread {

    @Override
    public void run() {
        for(int i=0;i<5;i++){
            //System.out.println(Thread.currentThread().getName()+"-------"+DBOperation.getInstance().hashCode());
            //System.out.println(Thread.currentThread().getName()+"-------"+DBOperation.getInstance().getInfluxDB().hashCode());
            System.out.println(Thread.currentThread().getName()+"-------"+i);
            try {
                if (i == 3) {
                    int a = 1 / 0;
                }
            }catch(ArithmeticException e){
                e.printStackTrace();
            }
        }
        System.out.println("come on");
    }

    public static void main(String[] args){
        MyThread[] mts=new MyThread[1];
        for(int i=0;i<mts.length;i++){
            mts[i]=new MyThread();
        }
        for(int j=0;j<mts.length;j++){
            mts[j].start();
        }
    }
}
