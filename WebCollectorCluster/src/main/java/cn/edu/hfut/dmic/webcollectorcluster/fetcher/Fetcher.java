/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.fetcher;

import cn.edu.hfut.dmic.webcollectorcluster.crawler.Crawler;
import cn.edu.hfut.dmic.webcollectorcluster.generator.Generator;
import cn.edu.hfut.dmic.webcollectorcluster.generator.GeneratorFactory;
import cn.edu.hfut.dmic.webcollectorcluster.generator.Merge;
import cn.edu.hfut.dmic.webcollectorcluster.generator.RecordGenerator;
import cn.edu.hfut.dmic.webcollectorcluster.handler.Handler;
import cn.edu.hfut.dmic.webcollectorcluster.handler.HandlerFactory;
import cn.edu.hfut.dmic.webcollectorcluster.handler.Message;
import cn.edu.hfut.dmic.webcollectorcluster.model.Content;
import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollectorcluster.model.Page;
import cn.edu.hfut.dmic.webcollectorcluster.model.WebWritable;
import cn.edu.hfut.dmic.webcollectorcluster.net.Request;
import cn.edu.hfut.dmic.webcollectorcluster.net.RequestFactory;
import cn.edu.hfut.dmic.webcollectorcluster.net.Response;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParseData;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParseResult;
import cn.edu.hfut.dmic.webcollectorcluster.parser.Parser;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParserFactory;

import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import cn.edu.hfut.dmic.webcollectorcluster.util.HandlerUtils;
import cn.edu.hfut.dmic.webcollectorcluster.util.LogUtils;

import java.io.IOException;


import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author hu
 */
public class Fetcher extends Configured implements Tool, MapRunnable<Text, CrawlDatum, Text, WebWritable> {

    private Handler handler = null;
    private RequestFactory requestFactory = null;
    private ParserFactory parserFactory = null;
    private GeneratorFactory generatorFactory = null;
    private AtomicInteger activeThreads;
    private FetchQueue fetchQueue;
    private QueueFeeder feeder;
    private OutputCollector<Text, WebWritable> output;
    private int retry = 3;
    private AtomicInteger spinWaiting;
    private AtomicLong lastRequestStart;
    public static final int FETCH_SUCCESS = 1;
    /**
     *
     */
    public static final int FETCH_FAILED = 2;
    private boolean isContentStored = true;
    private boolean parsing = true;

    public Fetcher(Configuration conf) {
        super(conf);
        try {
            isContentStored=conf.getBoolean("fetcher.store.content", false);
            String requestFactoryClass = conf.get("plugin.request.factory.class");
            String parseFactoryClass = conf.get("plugin.parser.factory.class");
            String generatorFactoryClass = conf.get("plugin.generator.factory.class");
            String handlerFactoryClass = conf.get("plugin.fetchhandler.factory.class");
            requestFactory = (RequestFactory) Class.forName(requestFactoryClass).newInstance();
            parserFactory = (ParserFactory) Class.forName(parseFactoryClass).newInstance();
            generatorFactory = (GeneratorFactory) Class.forName(generatorFactoryClass).newInstance();
            HandlerFactory handlerFactory = (HandlerFactory) Class.forName(handlerFactoryClass).newInstance();
            setHandler(handlerFactory.createHandler());
        } catch (Exception ex) {
           LogUtils.getLogger().info("Exception",ex);
        }

    }

    public Fetcher() {
        this(CrawlerConfiguration.create());
    }

    public static class FetchItem {

        public CrawlDatum datum;

        public FetchItem(CrawlDatum datum) {
            this.datum = datum;
        }
    }

  
    public static class FetchQueue {

      
        public AtomicInteger totalSize = new AtomicInteger(0);
      
        public List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());

        public synchronized void clear() {
            queue.clear();
        }

        public int getSize() {
            return queue.size();
        }

        public void addFetchItem(FetchItem item) {
            if (item == null) {
                return;
            }
            queue.add(item);
            totalSize.incrementAndGet();
        }

        public synchronized FetchItem getFetchItem() {
            if (queue.size() == 0) {
                return null;
            }
            return queue.remove(0);
        }

        public synchronized void dump() {
            for (int i = 0; i < queue.size(); i++) {
                FetchItem it = queue.get(i);
                LogUtils.getLogger().info(" " + i + ". " + it.datum.getUrl());
            }
        }
    }

    public static class QueueFeeder extends Thread {

        public FetchQueue queue;
        public Generator generator;
        public int size;

        public QueueFeeder(Generator generator, FetchQueue queue, int size) {
            this.queue = queue;
            this.generator = generator;
            this.size = size;
        }

        @Override
        public void run() {
            int generateMax = CrawlerConfiguration.create().getInt("generator.max", -1);
            int sum = 0;
            boolean hasMore = true;

            while (hasMore) {
                if (generateMax != -1) {
                    if (sum >= generateMax) {
                        break;
                    }
                }
                int feed = size - queue.getSize();
                if (feed <= 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    continue;
                }
                while (feed > 0 && hasMore) {
                    CrawlDatum datum = generator.next();
                    hasMore = (datum != null);
                    if (hasMore) {
                        queue.addFetchItem(new FetchItem(datum));
                        sum++;
                        feed--;
                    }
                }
            }
        }
    }

    private class FetcherThread extends Thread {

        @Override
        public void run() {
            activeThreads.incrementAndGet();
            FetchItem item = null;
            try {
                while (true) {
                    try {
                        item = fetchQueue.getFetchItem();
                        if (item == null) {
                            if (feeder.isAlive() || fetchQueue.getSize() > 0) {
                                spinWaiting.incrementAndGet();
                                try {
                                    Thread.sleep(500);
                                } catch (Exception ex) {
                                }
                                spinWaiting.decrementAndGet();
                                continue;
                            } else {
                                return;
                            }
                        }
                        lastRequestStart.set(System.currentTimeMillis());
                        CrawlDatum crawldatum = new CrawlDatum();
                        String url = item.datum.getUrl();
                        crawldatum.setUrl(url);
                        Request request = requestFactory.createRequest(url);
                        Response response = null;
                        for (int i = 0; i <= retry; i++) {
                            if (i > 0) {
                                LogUtils.getLogger().info("retry " + i + "th " + url);
                            }
                            try {
                                response = request.getResponse(crawldatum);
                                break;
                            } catch (Exception ex) {
                            }
                        }
                        crawldatum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
                        crawldatum.setFetchTime(System.currentTimeMillis());
                        Page page = new Page();
                        page.setUrl(url);
                        page.setFetchTime(crawldatum.getFetchTime());
                        if (response == null) {
                            LogUtils.getLogger().info("failed " + url);
                            HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_FAILED, page), true);
                            continue;
                        }
                        page.setResponse(response);
                        LogUtils.getLogger().info("fetch " + url);
                        String contentType = response.getContentType();
                        if (parsing) {
                            try {
                                Parser parser = parserFactory.createParser(url, contentType);
                                if (parser != null) {
                                    ParseResult parseresult = parser.getParse(page);
                                    page.setParseResult(parseresult);
                                }
                            } catch (Exception ex) {
                                LogUtils.getLogger().info("Exception", ex);
                            }
                        }
                        output.collect(new Text(crawldatum.getUrl()), new WebWritable(crawldatum));
                        
                        if (isContentStored && response.getContent() != null) {
                            Content content = new Content();
                            content.contentType = "html";
                            content.content = response.getContent();
                            output.collect(new Text(crawldatum.getUrl()), new WebWritable(content));
                        }
                        if (page.getParseResult() != null) {
                            ParseData parseData = page.getParseResult().getParsedata();
                            if (parseData != null && parseData.getUrl() != null) {
                                output.collect(new Text(crawldatum.getUrl()), new WebWritable(parseData));
                            }
                        }
                        HandlerUtils.sendMessage(handler, new Message(Fetcher.FETCH_SUCCESS, page), true);
                    } catch (Exception ex) {
                        LogUtils.getLogger().info("Exception", ex);
                    }
                }
            } catch (Exception ex) {
                LogUtils.getLogger().info("Exception", ex);
            } finally {
                activeThreads.decrementAndGet();
            }
        }
    }

    @Override
    public void run(RecordReader<Text, CrawlDatum> reader, OutputCollector<Text, WebWritable> oc, Reporter rprtr) throws IOException {
        fetchQueue = new FetchQueue();
        Generator generator = new RecordGenerator(reader);
        generator = generatorFactory.createGenerator(generator);
        feeder = new QueueFeeder(generator, fetchQueue, 1000);
        feeder.start();
        this.output = oc;
        lastRequestStart = new AtomicLong(System.currentTimeMillis());
        activeThreads = new AtomicInteger(0);
        spinWaiting = new AtomicInteger(0);
        long requestMaxInterval=getConf().getLong("fetcher.request.maxinterval", 60000);
        int threads = getConf().getInt("fetcher.threads", 30);
        for (int i = 0; i < threads; i++) {
            FetcherThread thread = new FetcherThread();
            thread.start();
        }
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
            LogUtils.getLogger().info("-activeThreads=" + activeThreads.get()
                    + ", spinWaiting=" + spinWaiting.get() + ", fetchQueue.size="
                    + fetchQueue.getSize());
            if (!feeder.isAlive() && fetchQueue.getSize() < 5) {
                fetchQueue.dump();
            }
            if ((System.currentTimeMillis() - lastRequestStart.get()) > requestMaxInterval) {
                LogUtils.getLogger().info("Aborting with " + activeThreads + " hung threads.");
                return;
            }
        } while (activeThreads.get() > 0);
        feeder.stop();
        fetchQueue.clear();
    }

    @Override
    public void configure(JobConf jc) {
        setConf(jc);

    }

    public static void main(String[] args) throws IOException, Exception {
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf jc = new JobConf(getConf());
        jc.setJarByClass(Fetcher.class);
        jc.setInputFormat(SequenceFileInputFormat.class);
        Path input = new Path(args[0], "current");
        Path output = new Path(args[1]);
        Configuration conf = CrawlerConfiguration.create();
        FileSystem fs = output.getFileSystem(conf);
        if (fs.exists(output)) {
            fs.delete(output);
        }
        FileInputFormat.addInputPath(jc, input);
        FileOutputFormat.setOutputPath(jc, output);

        jc.setMapOutputKeyClass(Text.class);
        jc.setMapOutputValueClass(WebWritable.class);

        jc.setMapRunnerClass(Fetcher.class);
        jc.setOutputFormat(FetcherOutputFormat.class);

        JobClient.runJob(jc);
        return 0;
    }

    public RequestFactory getRequestFactory() {
        return requestFactory;
    }

    public void setRequestFactory(RequestFactory requestFactory) {
        this.requestFactory = requestFactory;
    }

    public ParserFactory getParserFactory() {
        return parserFactory;
    }

    public void setParserFactory(ParserFactory parserFactory) {
        this.parserFactory = parserFactory;
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public Handler getHandler() {
        return handler;
    }

    public void setHandler(Handler handler) {
        this.handler = handler;
    }
}
