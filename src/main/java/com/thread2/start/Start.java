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

    static KProducer producer=new KProducer();

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        server.setHandler(new ReqHandler());
        server.start();
        server.join();
    }

    public static class ReqHandler extends AbstractHandler{
        @Override
        public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
            String line = null;
            StringBuffer json = new StringBuffer();
            BufferedReader content = httpServletRequest.getReader();
            while ((line = content.readLine()) != null) {
                 dealReq(line);
            }
            content.close();
        }
    }

    public static void dealReq(String s){
        try {
            producer.produce(s);
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
