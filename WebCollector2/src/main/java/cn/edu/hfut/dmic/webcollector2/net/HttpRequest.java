/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.net;

import cn.edu.hfut.dmic.webcollector2.crawl.CrawlDatum;
import cn.edu.hfut.dmic.webcollector2.util.Config;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author hu
 */
public class HttpRequest implements Request{

    public URL url;

    @Override
    public URL getURL() {
        return url;
    }

    @Override
    public void setURL(URL url) {
        this.url=url;
    }

    @Override
    public Response getResponse(CrawlDatum datum) throws IOException {  
        HttpResponse response=new HttpResponse(url);
        HttpURLConnection con;
        /*
        if(proxy==null){
            con=(HttpURLConnection) _URL.openConnection();
        }else{
            con=(HttpURLConnection) _URL.openConnection(proxy);
        }
                */
        con=(HttpURLConnection) url.openConnection();
        con.setDoInput(true);
        con.setDoOutput(true);
        
        
        InputStream is;
  
        response.setCode(con.getResponseCode());
        if(response.getCode()!=200){
            return response;
        }
        
        is=con.getInputStream();

        byte[] buf = new byte[2048];
        int read;
        int sum=0;
        int maxsize=Config.maxsize;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((read = is.read(buf)) != -1) {
            if(maxsize>0){
            sum=sum+read;
                if(sum>maxsize){
                    read=maxsize-(sum-read);
                    bos.write(buf, 0, read);                    
                    break;
                }
            }
            bos.write(buf, 0, read);
        }

        is.close();       
        
        response.content=bos.toByteArray();
        response.headers= con.getHeaderFields();
        return response;
    }
    

    public static void main(String[] args) throws Exception{
        Request request=RequestFactory.createRequest("http://www.xinhuanet.com");
        CrawlDatum datum=new CrawlDatum();
        Response response=request.getResponse(datum);
        System.out.println("status="+datum.status);
        System.out.println("code="+response.getCode());
        System.out.println(response.getHeaders());
    }
    
    
}
