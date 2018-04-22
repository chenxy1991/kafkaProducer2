package com.thread2.start;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;



import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.thread2.Producer.KProducer;

//通过python脚本向自定义的jetty服务器转发http请求，参数为从promethus中获取的数据

public class Start {

    //定义生产者
    static KProducer producer=new KProducer();

    //启动jetty服务器拦截web请求
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        server.setHandler(new ReqHandler());
        server.start();
        server.join();
    }

    //ReqHandler处理拦截到的请求
    public static class ReqHandler extends AbstractHandler{
        @Override
        public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
            String line = null;
            StringBuffer json = new StringBuffer();
            BufferedReader content = httpServletRequest.getReader();
            List<MetricUnit> messages=new ArrayList<MetricUnit>();
            while ((line = content.readLine()) != null) {
                json.append(line);
            }
            JSONObject object = JSON.parseObject(json.toString());
            JSONObject data = (JSONObject) object.get("data");
            JSONArray jsonArray = data.getJSONArray("result");
            messages = JSON.parseArray(jsonArray.toJSONString(),MetricUnit.class);
            dealReq(messages);
            content.close();
        }
    }

    /*public static void dealReq(String s){
        try {
           // producer.produce(s);                 //调用produce方法发送消息到kafka
           // System.out.println(s);
        }catch(Exception e){
            e.printStackTrace();
        }
    }*/

    public static void dealReq(List<MetricUnit> s){
        try {
            // producer.produce(s);                 //调用produce方法发送消息到kafka
            // System.out.println(s.toString());
            producer.produce(s);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
