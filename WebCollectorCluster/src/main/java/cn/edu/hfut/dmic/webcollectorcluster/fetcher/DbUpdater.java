/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollectorcluster.fetcher;

import cn.edu.hfut.dmic.webcollectorcluster.generator.Merge;
import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author hu
 */
public class DbUpdater extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        
        Path crawldb=new Path(args[0]);
        Path segmentPath=new Path(args[1]);
        Job job=Merge.createJob(CrawlerConfiguration.create(),crawldb);
        job.setJarByClass(DbUpdater.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(segmentPath,"fetch"));
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(segmentPath,"parse_temp"));
        job.waitForCompletion(true);
        Merge.install(crawldb);
        return 0;
    }
    
 
    
}
