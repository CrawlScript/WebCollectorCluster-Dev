/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.crawl;

import cn.edu.hfut.dmic.webcollectorcluster.fetcher.DbUpdater;
import cn.edu.hfut.dmic.webcollectorcluster.fetcher.Fetcher;
import cn.edu.hfut.dmic.webcollectorcluster.fetcher.SegmentUtils;
import cn.edu.hfut.dmic.webcollectorcluster.generator.Generator;
import cn.edu.hfut.dmic.webcollectorcluster.generator.GeneratorFactory;
import cn.edu.hfut.dmic.webcollectorcluster.generator.Injector;
import cn.edu.hfut.dmic.webcollectorcluster.generator.IntervalFilter;
import cn.edu.hfut.dmic.webcollectorcluster.generator.URLRegexFilter;
import cn.edu.hfut.dmic.webcollectorcluster.handler.Handler;
import cn.edu.hfut.dmic.webcollectorcluster.handler.HandlerFactory;
import cn.edu.hfut.dmic.webcollectorcluster.handler.Message;
import cn.edu.hfut.dmic.webcollectorcluster.model.Page;
import cn.edu.hfut.dmic.webcollectorcluster.net.HttpRequest;
import cn.edu.hfut.dmic.webcollectorcluster.net.Request;
import cn.edu.hfut.dmic.webcollectorcluster.net.RequestFactory;
import cn.edu.hfut.dmic.webcollectorcluster.parser.HtmlParser;
import cn.edu.hfut.dmic.webcollectorcluster.parser.Parser;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParserFactory;
import cn.edu.hfut.dmic.webcollectorcluster.util.Config;
import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import cn.edu.hfut.dmic.webcollectorcluster.util.LogUtils;
import java.net.URL;
import java.util.ArrayList;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author hu
 */
public class Crawler implements RequestFactory, ParserFactory, HandlerFactory, GeneratorFactory {

    public Crawler() {

    }

    Path crawlDir;
    Path crawldb;
    Path segments;
    private boolean resumable=false;

    public Crawler(String crawlPath) {
        this.crawlDir = new Path(crawlPath);
        crawldb = new Path(crawlDir, "crawldb");
        segments = new Path(crawlDir, "segments");
    }

    /**
     * 生成处理抓取消息的Handler，默认通过Crawler的visit方法来处理成功抓取的页面， 通过failed方法来处理失败抓取的页面
     *
     * @return 处理抓取消息的Handler
     */
    public Handler createHandler() {
        Handler fetchHandler = new Handler() {
            @Override
            public void handleMessage(Message msg) {
                Page page = (Page) msg.obj;
                switch (msg.what) {
                    case Fetcher.FETCH_SUCCESS:

                        visit(page);
                        break;
                    case Fetcher.FETCH_FAILED:
                        failed(page);
                        break;
                    default:
                        break;

                }
            }
        };
        return fetchHandler;
    }
    
    

    /**
     * 爬取成功时执行的方法
     *
     * @param page 成功爬取的网页/文件
     */
    public void visit(Page page) {
        
        
        System.out.println(page.getDoc().title());
    }

    /**
     * 爬取失败时执行的方法
     *
     * @param page 爬取失败的网页/文件
     */
    public void failed(Page page) {

    }

    private ArrayList<String> seeds = new ArrayList<String>();
    //private Fetcher fetcher;

    public void inject() throws Exception {
        if(seeds.size()>0){
        Injector injector = new Injector();
        injector.inject(crawldb, seeds);
        }
    }

    public void start(int depth) throws Exception {
        
        
        if(!resumable){
            FileSystem fs=crawlDir.getFileSystem(CrawlerConfiguration.create());
            if(fs.exists(crawlDir)){
                fs.delete(crawlDir);
            }
        }
        
        

        inject();

        for (int i = 0; i < depth; i++) {
            LogUtils.getLogger().info("starting depth " + (i + 1));
            String segmentName = SegmentUtils.createSegmengName();
            Path segmentPath = new Path(segments, segmentName);

           // ArrayList<String> fetchArgsList=new ArrayList<String>();
            // fetchArgsList.add(crawldb.toString());
            //fetchArgsList.add(segmentPath.toString());
            //fetchArgsList.addAll(regexs);
            //String[] fetch_args=fetchArgsList.toArray(new String[fetchArgsList.size()]);
            String[] args = new String[]{crawldb.toString(), segmentPath.toString()};
            ToolRunner.run(new Fetcher(), args);
            ToolRunner.run(new DbUpdater(), args);
        }

    }

    /**
     * 添加一个种子url
     *
     * @param seed 种子url
     */
    public void addSeed(String seed) {
        seeds.add(seed);
    }

    /**
     * 添加一个正则过滤规则
     *
     * @param regex 正则过滤规则
     */
    public void addRegex(String regex) {
        if (!URLRegexFilter.rules.contains(regex)) {
            URLRegexFilter.rules.add(regex);
        }
    }

    public static void main(String[] args) throws Exception {
        
        Config.threads=50;
        
        Crawler crawler = new Crawler("/home/hu/data/clustertest1");
        crawler.addSeed("https://ruby-china.org/");
        crawler.addRegex("https://ruby-china.org.*");
        //crawler.addRegex("-http://www.zhihu.com/people.*");
        //crawler.addRegex("-http://www.zhihu.com/inbox.*");
        crawler.addRegex("-.*jpg.*");
        crawler.addRegex("-.*png.*");
        crawler.addRegex("-.*#.*");
        crawler.addRegex("-.*gif.*");
        crawler.addRegex("-.*\\?.*");
        crawler.start(50);
    }

    @Override
    public Request createRequest(String url) throws Exception {
        HttpRequest request = new HttpRequest();
        URL _URL = new URL(url);
        request.setURL(_URL);
        //request.setProxy(proxy);
        //request.setConnectionConfig(conconfig);
        return request;
    }

    /**
     * 根据网页的url和contentType，来创建Parser(解析器)，可以通过Override这个方法来自定义Parser
     *
     * @param url
     * @param contentType
     * @return 实现Parser接口的对象
     * @throws Exception
     */
    @Override
    public Parser createParser(String url, String contentType) throws Exception {
        if (contentType == null) {
            return null;
        }
        if (contentType.contains("text/html")) {
            return new HtmlParser(Config.topN);
        }
        return null;
    }

    /*
     @Override
     public Generator createGenerator(Generator generator) {
     return new URLRegexFilter(generator, regexs);
     }
     */
    @Override
    public Generator createGenerator(Generator generator) {
        //Generator g=new URLRegexFilter(new IntervalFilter(generator));
        //return g;
        return new URLRegexFilter(new IntervalFilter(generator));
    }

    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

    
    
}
