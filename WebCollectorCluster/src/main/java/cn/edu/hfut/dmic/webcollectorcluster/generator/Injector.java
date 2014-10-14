/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.generator;

import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import java.io.IOException;
import java.util.ArrayList;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;



import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author hu
 */
public class Injector {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path crawldb = new Path("/home/hu/data/cluster/crawldb");
        Injector injector = new Injector();
        ArrayList<String> urls = new ArrayList<String>();
        urls.add("http://www.xinhuanet.com/");
        urls.add("http://www.sina.com");
        injector.inject(crawldb, urls);
    }

    public void inject(Path crawldb, ArrayList<String> urls) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration config = CrawlerConfiguration.create();
        FileSystem fs = crawldb.getFileSystem(config);
        Path tempdb = new Path(crawldb, "temp");
        if (fs.exists(tempdb)) {
            fs.delete(tempdb);
        }
        //AvroKeyRecordWriter<CrawlDatum> writer=new AvroKeyRecordWriter<CrawlDatum>
        
        SequenceFile.Writer writer=new SequenceFile.Writer(fs, config, new Path(tempdb,"info.avro"), Text.class, CrawlDatum.class);
        for (String url : urls) {
            CrawlDatum crawldatum = new CrawlDatum();
            crawldatum.setUrl(url);
            crawldatum.setStatus(CrawlDatum.STATUS_DB_INJECTED);
            writer.append(new Text(url),crawldatum);
            System.out.println("inject:" + url);
        }
        writer.close();
        /*
        DbWriter<CrawlDatum> writer;
        writer = new DbWriter<CrawlDatum>(CrawlDatum.class, new Path(tempdb,"info.avro"), config, fs, false);
        for (String url : urls) {
            CrawlDatum crawldatum = new CrawlDatum();
            crawldatum.setUrl(url);
            crawldatum.setStatus(CrawlDatum.STATUS_DB_INJECTED);
            writer.write(crawldatum);
            System.out.println("inject:" + url);
        }
        writer.close();
*/
        Job job = Merge.createJob(crawldb);
        FileInputFormat.addInputPath(job, tempdb);

        job.waitForCompletion(true);
        Merge.install(job, crawldb);
        
        if(fs.exists(tempdb)){
            fs.delete(tempdb);
        }

    }

}
