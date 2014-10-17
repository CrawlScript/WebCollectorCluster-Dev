/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cn.edu.hfut.dmic.webcollectorcluster.fetcher;

import cn.edu.hfut.dmic.webcollectorcluster.model.Content;
import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import cn.edu.hfut.dmic.webcollectorcluster.model.Link;
import cn.edu.hfut.dmic.webcollectorcluster.model.WebWritable;
import cn.edu.hfut.dmic.webcollectorcluster.parser.ParseData;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
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
        Path fetchPath = new Path(outputPath, "fetch/info");
        Path contentPath = new Path(outputPath, "content/info");
        Path parseDataPath = new Path(outputPath, "parse_data/info");
        Path parseTempPath = new Path(outputPath, "parse_temp/info");
        final SequenceFile.Writer fetchOut = new SequenceFile.Writer(fs, conf, fetchPath, Text.class, CrawlDatum.class);
        final SequenceFile.Writer contentOut = new SequenceFile.Writer(fs, conf, contentPath, Text.class, Content.class);
        final SequenceFile.Writer parseDataOut = new SequenceFile.Writer(fs, conf, parseDataPath, Text.class, ParseData.class);
        final SequenceFile.Writer parseTempOut = new SequenceFile.Writer(fs, conf, parseTempPath, Text.class, CrawlDatum.class);
        return new RecordWriter<Text, WebWritable>() {
            @Override
            public void write(Text key, WebWritable value) throws IOException {
                Writable w = value.get();
                if (w instanceof CrawlDatum) {
                    fetchOut.append(key, w);
                } else if (w instanceof Content) {
                    contentOut.append(key, w);
                } else if (w instanceof ParseData) {
                    parseDataOut.append(key, w);
                    ParseData parseData = (ParseData) w;
                    if (parseData.getLinks() != null) {
                        for (Link link : parseData.getLinks()) {
                            CrawlDatum datum = new CrawlDatum();
                            datum.setUrl(link.getUrl());
                            datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                            datum.setFetchTime(CrawlDatum.FETCHTIME_UNDEFINED);
                            parseTempOut.append(new Text(datum.getUrl()), datum);
                        }
                    }
                }
            }

            @Override
            public void close(Reporter rprtr) throws IOException {
                fetchOut.close();
                contentOut.close();
                parseDataOut.close();
                parseTempOut.close();
            }
        };
    }

    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {
    }
}
