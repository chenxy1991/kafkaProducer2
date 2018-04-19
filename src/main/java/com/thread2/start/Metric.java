package com.thread2.start;

public class Metric {

    private String cluster;
    private String instance;
    private String __name__;
    private String host;
    private String proj;
    private String job;
    private String m;

    public Metric(String cluster, String instance, String __name__, String host, String proj, String job, String m) {
        this.cluster = cluster;
        this.instance = instance;
        this.__name__ = __name__;
        this.host = host;
        this.proj = proj;
        this.job = job;
        this.m = m;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String get__name__() {
        return __name__;
    }

    public void set__name__(String __name__) {
        this.__name__ = __name__;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getProj() {
        return proj;
    }

    public void setProj(String proj) {
        this.proj = proj;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public String getM() {
        return m;
    }

    public void setM(String m) {
        this.m = m;
    }

    @Override
    public String toString(){
        return "cluster="+this.cluster+",instance="+this.instance+",metricName="+this.__name__+",host="+this.host+",proj="+this.proj+",job="+this.job+",m="+this.m;
    }

}
