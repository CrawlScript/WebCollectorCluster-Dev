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


import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author hu
 */
public class Injector {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Path crawldb = new Path("hdfs://localhost:9000/cluster3");
        Injector injector = new Injector();
        ArrayList<String> urls = new ArrayList<String>();
        urls.add("https://ruby-china.org/");
     
        injector.inject(crawldb, urls);
    }

    public void inject(Path crawlDir, ArrayList<String> urls) throws IOException, InterruptedException, ClassNotFoundException, Exception {
        Path crawldb=new Path(crawlDir,"crawldb");
        Configuration config = CrawlerConfiguration.create();
         System.out.println(config.get("mapred.jar"));
        FileSystem fs = crawldb.getFileSystem(config);
        Path tempdb = new Path(crawldb, "temp");
        if (fs.exists(tempdb)) {
            fs.delete(tempdb);
        }
        
        
        SequenceFile.Writer writer=new SequenceFile.Writer(fs, config, new Path(tempdb,"info.avro"), Text.class, CrawlDatum.class);
        for (String url : urls) {
            CrawlDatum crawldatum = new CrawlDatum();
            crawldatum.setUrl(url);
            crawldatum.setStatus(CrawlDatum.STATUS_DB_INJECTED);
            writer.append(new Text(url),crawldatum);
            System.out.println("inject:" + url);
        }
        writer.close();
       
     
        String[] args=new String[]{crawldb.toString(),tempdb.toString()};
        
        ToolRunner.run(CrawlerConfiguration.create(), new Merge(),args);
        Merge.install(crawldb);
        
        if(fs.exists(tempdb)){
            fs.delete(tempdb);
        }

    }

}
