/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector2.fetcher;

import cn.edu.hfut.dmic.webcollector2.crawl.CrawlDatum;
import cn.edu.hfut.dmic.webcollector2.crawl.WebWritable;
import cn.edu.hfut.dmic.webcollector2.net.Request;
import cn.edu.hfut.dmic.webcollector2.net.RequestFactory;
import cn.edu.hfut.dmic.webcollector2.net.Response;
import cn.edu.hfut.dmic.webcollector2.util.CrawlerConfiguration;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author hu
 */
public class Fetcher extends Configured implements Tool,MapRunnable<LongWritable, Text, Text, WebWritable> {

    public AtomicInteger activeThreads = new AtomicInteger(0);
    FetchQueue fetchQueue;
    QueueFeeder feeder;
    OutputCollector<Text, WebWritable> output;

    

   

    public static class QueueFeeder extends Thread {

        public RecordReader<LongWritable, Text> reader;
        public FetchQueue queue;
        public int size;

        public QueueFeeder(RecordReader<LongWritable, Text> reader, FetchQueue queue, int size) {
            this.reader = reader;
            this.queue = queue;
            this.size = size;
        }

        @Override
        public void run() {
            boolean hasMore = true;
            while (hasMore) {

                int feed = size - queue.getSize();
                if (feed <= 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                    }
                    continue;
                } else {

                    while (feed > 0 && hasMore) {
                        LongWritable k = new LongWritable();
                        Text v = new Text();
                        try {

                            hasMore = reader.next(k, v);
                            if (hasMore) {
                                queue.addFetchItem(new FetchItem(new URL(v.toString()), null));
                                feed--;
                            }

                        } catch (IOException ex) {
                            Logger.getLogger(Fetcher.class.getName()).log(Level.SEVERE, null, ex);
                            return;
                        }
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
                    item = fetchQueue.getFetchItem();
                    if (item == null) {
                        if (feeder.isAlive() || fetchQueue.getSize() > 0) {
                            try {
                                Thread.sleep(500);
                            } catch (Exception ex) {
                            }
                            continue;
                        } else {
                            return;
                        }
                    }
                    Request request=RequestFactory.createRequest(item.url.toString());
                    CrawlDatum datum=new CrawlDatum();
                    
                    int retries=0;
                    Response response=null;
                    for(int i=0;i<=3;i++){
                        try{
                            response=request.getResponse(datum);
                            break;
                        }catch(Exception ex){
                            retries++;
                        }
                    }
                    
                    if(response==null){
                        datum.status=CrawlDatum.STATUS_DB_UNFETCHED;
                    }else{
                        if(response.getCode()==200)
                            datum.status=CrawlDatum.STATUS_DB_FETCHED;
                        else
                            datum.status=CrawlDatum.STATUS_DB_UNFETCHED;
                    }
                    datum.fetchTime=System.currentTimeMillis();
                    datum.retries=retries;
                    
                    output.collect(new Text(item.url.toString()),new WebWritable(datum));
                    if(response.getContent()!=null){
                        Content content=new Content();
                        content.contentType="html";
                        content.content=response.getContent();
                        output.collect(new Text(item.url.toString()),new WebWritable(content));
                    }
                        
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {

                activeThreads.decrementAndGet();
            }

        }
    }

    public static class FetchItem {

        public URL url;
        public CrawlDatum datum;

        public FetchItem(URL url, CrawlDatum datum) {
            this.url = url;
            this.datum = datum;
        }

    }

    public static class FetchQueue {

        public AtomicInteger totalSize = new AtomicInteger(0);
        public List<FetchItem> queue = Collections.synchronizedList(new LinkedList<FetchItem>());

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

        public FetchItem getFetchItem() {
            if (queue.size() == 0) {
                return null;
            }
            return queue.remove(0);
        }
    }

   

    @Override
    public void run(RecordReader<LongWritable, Text> reader, OutputCollector<Text, WebWritable> oc, Reporter rprtr) throws IOException {
        fetchQueue = new FetchQueue();
        feeder = new QueueFeeder(reader, fetchQueue, 2);
        feeder.start();
        this.output=oc;
        for (int i = 0; i < 10; i++) {
            FetcherThread thread = new FetcherThread();
            thread.start();

        }

        do {
            try {
                Thread.sleep(1000);
            } catch (Exception ex) {
            }
        } while (activeThreads.get() > 0);
    }

    @Override
    public void configure(JobConf jc) {
        setConf(jc);
    }
    
    
    public static void main(String[] args) throws IOException, Exception {
        int res=ToolRunner.run(CrawlerConfiguration.create(), new Fetcher(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] strings) throws Exception {
        
        JobConf jc = new JobConf();
        jc.setInputFormat(TextInputFormat.class);

        Path input = new Path("/home/hu/code/hadooptest/urls");
        Path output = new Path("/home/hu/code/hadooptest/output");
        Configuration conf = new Configuration();
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

}
