/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.generator;

import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author hu
 */
public class Merge extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        return -1;
    }

    public static Job createJob(Path crawldb) throws IOException {

        Job job = new Job();

        job.setJarByClass(Merge.class);

        Path newdb = new Path(crawldb, "new");
        
        
        Path currentdb = new Path(crawldb, "current");

        FileSystem fs = crawldb.getFileSystem(CrawlerConfiguration.create());
        if (fs.exists(currentdb)) {
            FileInputFormat.addInputPath(job, currentdb);
        }
        
        if(fs.exists(newdb)){
            fs.delete(newdb);
        }

        FileOutputFormat.setOutputPath(job, newdb);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        job.setMapperClass(MergeMap.class);
        job.setReducerClass(MergeReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CrawlDatum.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        return job;
    }

    public static void install(Job job, Path crawldb) throws IOException {
        FileSystem fs = crawldb.getFileSystem(CrawlerConfiguration.create());
        Path newdb = new Path(crawldb, "new");
        Path currentdb = new Path(crawldb, "current");
        Path olddb = new Path(crawldb, "old");
        if (fs.exists(currentdb)) {
            if (fs.exists(olddb)) {
                fs.delete(olddb);
            }
            fs.rename(currentdb, olddb);
        }
        fs.mkdirs(crawldb);
        fs.rename(newdb, currentdb);
    }

    public static class MergeMap extends Mapper<Text, CrawlDatum, Text, CrawlDatum> {

        @Override
        protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {

            context.write(new Text(key), value);
        }

    }

    public static class MergeReduce extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {

        @Override
        protected void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
            Iterator<CrawlDatum> ite = values.iterator();
            CrawlDatum temp = null;
            while (ite.hasNext()) {
                CrawlDatum nextDatum = ite.next();
                if (nextDatum.getStatus() == CrawlDatum.STATUS_DB_INJECTED) {
                    temp = nextDatum;
                    temp.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                    break;
                }
                if (temp == null) {
                    temp = nextDatum;
                    continue;
                }
                if (nextDatum.getStatus() > temp.getStatus()) {
                    temp = nextDatum;
                    continue;
                }
                if (nextDatum.getFetchTime() > temp.getFetchTime()) {
                    temp = nextDatum;
                }
            }
            if (temp != null) {
                context.write(key, temp);
            }

        }

    }
}
