/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollector2.fetcher;

import cn.edu.hfut.dmic.webcollector2.crawl.CrawlDatum;
import cn.edu.hfut.dmic.webcollector2.crawl.WebWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author hu
 */
public class FetcherOutputFormat implements OutputFormat<Text, WebWritable> {

    @Override
    public org.apache.hadoop.mapred.RecordWriter<Text, WebWritable> getRecordWriter(FileSystem fs, JobConf jc, String string, Progressable p) throws IOException {
        Configuration conf = jc;
        String outputPath = conf.get("mapred.output.dir");
        Path fetchPath = new Path(outputPath, CrawlDatum.FETCH_DIR_NAME);
        Path contentPath = new Path(outputPath, Content.CONTENT_DIR_NAME);

        final MapFile.Writer fetchOut = new MapFile.Writer(conf, fs, fetchPath.toString(), Text.class, CrawlDatum.class);
        final MapFile.Writer contentOut = new MapFile.Writer(conf, fs, contentPath.toString(), Text.class, Content.class);
        return new RecordWriter<Text, WebWritable>() {

            @Override
            public void write(Text key, WebWritable value) throws IOException {
                Writable w = value.get();
                if (w instanceof CrawlDatum) {
                    fetchOut.append(key, w);
                } else if (w instanceof Content) {
                    contentOut.append(key, w);
                }
            }

            @Override
            public void close(Reporter rprtr) throws IOException {
                fetchOut.close();
                contentOut.close();
            }

        };

    }

    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {
    }

}
