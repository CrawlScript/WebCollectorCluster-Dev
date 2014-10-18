/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.crawler;

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
import cn.edu.hfut.dmic.webcollectorcluster.util.ConnectionConfig;
import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import cn.edu.hfut.dmic.webcollectorcluster.util.LogUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
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
    public ArrayList<String> rules = new ArrayList<String>();
    Path crawlDir;
    Path crawldb;
    Path segments;
    private boolean resumable = false;

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

        LogUtils.getLogger().info(page.getDoc().title());
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
        if (seeds.size() > 0) {
            Injector injector = new Injector();
            injector.inject(crawlDir, seeds);
        }
    }

    public void start(int depth) throws Exception {

        Configuration conf = CrawlerConfiguration.create();
        FileSystem fs = crawlDir.getFileSystem(conf);

        if (!resumable) {
            if (fs.exists(crawlDir)) {
                fs.delete(crawlDir);
            }
        }

        inject();

        for (int i = 0; i < depth; i++) {
            LogUtils.getLogger().info("starting depth " + (i + 1));
            String segmentName = SegmentUtils.createSegmengName();
            Path segmentPath = new Path(segments, segmentName);

  
            String[] args = new String[]{crawldb.toString(), segmentPath.toString()};
            ToolRunner.run(CrawlerConfiguration.create(), new Fetcher(), args);
            ToolRunner.run(CrawlerConfiguration.create(), new DbUpdater(), args);
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
        if (!rules.contains(regex)) {
            rules.add(regex);
        }
    }

    public static void main(String[] args) throws Exception {

       // Crawler crawler = new Crawler("hdfs://localhost:9000/cluster9");
        Crawler crawler = new Crawler("/home/hu/data/cluster4");
        crawler.addSeed("http://www.xinhuanet.com/");
        
        crawler.start(50);
    }

    Integer maxsize = null;
    Long interval = null;
    Integer topN = null;

    public static class CommonConnectionConfig implements ConnectionConfig {

        String userAgent = null;
        String cookie = null;

        public CommonConnectionConfig() {
            Configuration conf = CrawlerConfiguration.create();
            userAgent = conf.get("http.agent.name");
            cookie = conf.get("http.cookie");
        }

        @Override
        public void config(HttpURLConnection con) {
            if (userAgent != null) {
                con.setRequestProperty("User-Agent", userAgent);
            }
            if (cookie != null) {
                con.setRequestProperty("Cookie", cookie);
            }
        }

    }

    @Override
    public Request createRequest(String url) throws Exception {
        if (maxsize == null) {
            maxsize = CrawlerConfiguration.create().getInt("http.content.limit", 65535);
        }

        HttpRequest request = new HttpRequest(maxsize);
        URL _URL = new URL(url);
        request.setURL(_URL);
        request.setConnectionConfig(new CommonConnectionConfig());
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

        if (topN == null) {
            topN = CrawlerConfiguration.create().getInt("parser.topN", -1);
        }
        if (contentType == null) {
            return null;
        }
        if (contentType.contains("text/html")) {
            return new HtmlParser(topN);
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

        if (interval == null) {
            interval = CrawlerConfiguration.create().getLong("generator.interval", -1);
        }
        try {
            Configuration conf = CrawlerConfiguration.create();

            InputStream regexIs = conf.getConfResourceAsInputStream("regex");
            BufferedReader br = new BufferedReader(new InputStreamReader(regexIs));
            ArrayList<String> regexRules = new ArrayList<String>();
            String line;
            while ((line = br.readLine()) != null) {
                regexRules.add(line);
            }

            //return new URLRegexFilter(generator, regexRules);
            return new URLRegexFilter(new IntervalFilter(generator, interval), regexRules);
        } catch (Exception ex) {
            LogUtils.getLogger().info("Exception", ex);
            return null;
        }

    }

    public boolean isResumable() {
        return resumable;
    }

    public void setResumable(boolean resumable) {
        this.resumable = resumable;
    }

}
