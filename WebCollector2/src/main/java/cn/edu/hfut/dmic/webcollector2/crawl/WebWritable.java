/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.crawl;

import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hu
 */
public class WebWritable extends GenericWritable{
    
    public WebWritable(){
        
    }
    
    public WebWritable(Writable instance){
        set(instance);
        
    }

    
    
    private static Class<? extends Writable>[] CLASSES=null;
    
    static{
        CLASSES=(Class<? extends Writable>[])new Class<?>[]{
            org.apache.hadoop.io.Text.class,
            cn.edu.hfut.dmic.webcollector2.crawl.CrawlDatum.class,
            cn.edu.hfut.dmic.webcollector2.fetcher.Content.class
            
        };
    }
    

    @Override
    protected Class<? extends Writable>[] getTypes() {
        return CLASSES;
    }
    
    
}
