package com.thread2.ConsumerThread;

public class Offset {

   private long initOffset;
   private long lastOffset;

    public long getInitOffset() {
        return initOffset;
    }

    public void setInitOffset(long initOffset) {
        this.initOffset = initOffset;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

}
