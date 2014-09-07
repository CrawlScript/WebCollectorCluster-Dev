/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.util;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author hu
 */
public class CrawlerConfiguration {
    
    public static Configuration create(){
         Configuration conf=new Configuration();
         conf.addResource("crawler-default.xml");
         return conf;
    }
    
    
}
