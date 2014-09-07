/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.net;

import cn.edu.hfut.dmic.webcollector2.crawl.CrawlDatum;
import java.net.URL;

/**
 *
 * @author hu
 */
public interface Request {
    public URL getURL();
    public void setURL(URL url);
    
    public Response getResponse(CrawlDatum datum) throws Exception;
}
