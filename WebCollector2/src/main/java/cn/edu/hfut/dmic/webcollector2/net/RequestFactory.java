/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.net;

import java.net.MalformedURLException;
import java.net.URL;

/**
 *
 * @author hu
 */
public class RequestFactory {
    
    public static Request createRequest(String url) throws MalformedURLException{
        Request request=new HttpRequest();
        URL _URL=new URL(url);
        request.setURL(_URL);
        return request;
                
    }
    
}
