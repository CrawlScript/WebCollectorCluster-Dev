/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.net;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hu
 */
public interface Response {
   
  public URL getUrl();

 
  public int getCode();


  public List<String> getHeader(String name);
 
  public Map<String,List<String>> getHeaders();
 
 
  public byte[] getContent();
  
}
