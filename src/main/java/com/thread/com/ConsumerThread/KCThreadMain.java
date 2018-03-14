package com.thread.com.ConsumerThread;

public class KCThreadMain {

    public static void main(String[] args) {

    int threadNum = 2;

    KCThreadCreate create = new KCThreadCreate(threadNum);
    create.start();

    }
}
