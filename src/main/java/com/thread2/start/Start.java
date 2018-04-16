package com.thread2.start;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import com.thread2.Producer.KProducer;

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
            while ((line = content.readLine()) != null) {
                 dealReq(line);                   //处理请求内容
            }
            content.close();
        }
    }

    public static void dealReq(String s){
        try {
            producer.produce(s);                 //调用produce方法发送消息到kafka
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
