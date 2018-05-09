package com.thread2.start;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.thread2.Producer.KProducer;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

//通过直接向promethus发送http请求的方式获取数据

public class HttpRequest {

    static KProducer producer = new KProducer();
    private static Logger log = LoggerFactory.getLogger("ProducerLog");

    public static void main(String[] args) {

        String strUrl = "http://132.121.204.6:9090/api/v1/query?query=";
        String param = "{__name__='cpu'}";
         // String param = "{__name__=~'^(mem|mem_info|cpu|cpu_info|host|load|misc|tcp|disk|disk_io|disk_info|disk_usage|net|net_info|SwapMemory|memory|proc_uptime|proc_count|proc_stat|proc_diskIO|proc_netIO|proc_tcp_count|proc_cpu|proc_memory_size|proc_thread_count)'}";
        int count = 0;
        List<MetricUnit> messages = new ArrayList<MetricUnit>();

        while (count <= 2) {
            try {
                String url = strUrl + URLEncoder.encode(param, "utf-8");
                HttpClient client = new DefaultHttpClient();
                //发送get请求
                HttpGet request = new HttpGet(url);
                HttpResponse response = client.execute(request);

                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    String strResult = EntityUtils.toString(response.getEntity());
                    messages=getMessage(strResult);
                    System.out.println("这是第"+count+"次拉取数据，拉取数据的条数为"+messages.size());
                    log.info("这是第[{}]次拉取数据，拉取数据的条数为[{}]",count,messages.size());
                    dealReq(messages);
                    count++;
                }
                else{
                    log.info("http请求返回状态码[{}],请求参数为[{}]",response.getStatusLine().getStatusCode(),param);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void dealReq(List<MetricUnit> s){
         try {
            // System.out.println(s.toString());
            producer.produce(s);   //调用produce方法发送消息到kafka
          }catch(Exception e){
            e.printStackTrace();
         }
      }

    public static List<MetricUnit> getMessage(String message){
          List<MetricUnit> messages = new ArrayList<MetricUnit>();
          JSONObject object = JSON.parseObject(message);
          JSONObject data = (JSONObject) object.get("data");
          JSONArray jsonArray = data.getJSONArray("result");
          messages = JSON.parseArray(jsonArray.toJSONString(), MetricUnit.class);
          return messages;
      }
}
