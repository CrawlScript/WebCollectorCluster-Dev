/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.fetcher;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author hu
 */
public class Test{
    
    public static void main(String[] args) throws IOException{
        
        Configuration conf1=new Configuration();
 
        conf1.addResource("crawler-default.xml");
        /*
        byte[] b=new byte[2000];
        int r=2000;
        while((r=is.read(b))!=-1){
            System.out.print(r);
        }
     */
        System.out.println(conf1.get("http.agent.name"));
         
        if(1==1)
            return;
        Job job=new Job();
        JobConf jc=new JobConf();
        System.out.println(jc.getMapRunnerClass().getName());
        if(1==1)
            return;
        
        Path input=new Path("/home/hu/code/hadooptest/urls");
        Path output=new Path("/home/hu/code/hadooptest/output");
        Configuration conf=new Configuration();
        FileSystem fs=output.getFileSystem(conf);
        if(fs.exists(output)){
            fs.delete(output);
        }
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        
    }
    
}
