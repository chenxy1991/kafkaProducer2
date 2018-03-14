package com.thread.com.ConsumerThread;

import java.util.ArrayList;
import java.util.List;

public class KCThreadCreate {

    private final int threadNum;
    private List<KCThread> consumerThreadList = new ArrayList<KCThread>();

    public KCThreadCreate(int threadNum){
       this.threadNum=threadNum;
        for(int i = 0; i< threadNum;i++){
            KCThread consumerThread = new KCThread();
            consumerThreadList.add(consumerThread);
        }
    }

    public void start(){
        for(KCThread kcThread:consumerThreadList){
            Thread thread=new Thread(kcThread);
            thread.start();
        }
    }


}
