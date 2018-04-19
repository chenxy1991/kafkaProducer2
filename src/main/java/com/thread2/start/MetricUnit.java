package com.thread2.start;

import java.util.List;

public class MetricUnit {

    private Metric metric;
    private String value;


    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }


    @Override
    public String toString(){
        return metric.toString()+","+value;
    }

    public String getTags(){
        return "cluster="+metric.getCluster()+",instance="+metric.getInstance()+",host="+metric.getHost()+",proj="+metric.getProj()+",job="+metric.getJob()+",m="+metric.getM();
    }

    public String getMetricName(){
        return metric.get__name__();
    }
}
