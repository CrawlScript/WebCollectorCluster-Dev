/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package cn.edu.hfut.dmic.webcollectorcluster.model;

import cn.edu.hfut.dmic.webcollectorcluster.net.Response;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParseResult;
import org.jsoup.nodes.Document;





/**
 * Page是爬取过程中，内存中保存网页爬取信息的一个容器，与CrawlDatum不同，Page只在内存中存
 * 放，用于保存一些网页信息，方便用户进行自定义网页解析之类的操作。在广度遍历器中，用户覆盖
 * 的visit(Page page)方法，就是通过Page将网页爬取/解析信息传递给用户的。经过http请求、解
 * 析这些流程之后，page内保存的内容会越来越多。
 * @author hu
 */
public class Page{
    private Response response=null;
    private String url=null;  
    private String html=null;
    private Document doc=null;  
    private long fetchTime;
    private ParseResult parseResult=null;
    
    /**
     * 设置http响应
     * @param response
     */
    public void setResponse(Response response){
        this.response=response;
    }
    
    /**
     * 返回存储的http响应
     * @return http响应
     */
    public Response getResponse(){
        return response;
    }
    
    /**
     * 返回网页/文件的内容
     * @return 网页/文件的内容
     */
    public byte[] getContent() {
        if(response==null)
            return null;
        return response.getContent();
    }

    /**
     * 返回网页的url
     * @return 网页的url
     */
    public String getUrl() {
        return url;
    }

    /**
     * 设置网页的url
     * @param url 网页的url
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * 返回网页的源码字符串
     * @return 网页的源码字符串
     */
    public String getHtml() {
        return html;
    }

    /**
     * 设置网页的源码字符串
     * @param html 网页的源码字符串
     */
    public void setHtml(String html) {
        this.html = html;
    }

    /**
     * 返回网页解析后的DOM树(Jsoup的Document对象)
     * @return 网页解析后的DOM树
     */
    public Document getDoc() {
        return doc;
    }

    /**
     * 设置网页解析后的DOM树(Jsoup的Document对象)
     * @param doc 网页解析后的DOM树
     */
    public void setDoc(Document doc) {
        this.doc = doc;
    }

    /**
     * 返回爬取时间
     * @return 爬取时间
     */
    public long getFetchTime() {
        return fetchTime;
    }

    /**
     * 设置爬取时间
     * @param fetchTime 爬取时间
     */
    public void setFetchTime(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    /**
     * 返回网页解析结果
     * @return 网页解析结果
     */
    public ParseResult getParseResult() {
        return parseResult;
    }

    /**
     * 设置网页解析结果
     * @param parseResult 网页解析结果
     */
    public void setParseResult(ParseResult parseResult) {
        this.parseResult = parseResult;
    }
    
    
    
    
}
