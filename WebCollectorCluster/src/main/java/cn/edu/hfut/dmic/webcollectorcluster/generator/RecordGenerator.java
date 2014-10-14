/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollectorcluster.generator;

import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 * @author hu
 */
public class RecordGenerator implements Generator{

    public RecordGenerator(RecordReader<Text, CrawlDatum> reader) {
        this.reader = reader;
    }
    
    
    
    RecordReader<Text, CrawlDatum> reader;

    @Override
    public CrawlDatum next() {
        Text text=new Text();
        CrawlDatum datum=new CrawlDatum();
        boolean hasMore;
        try {
            hasMore = reader.next(text, datum);
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
        if(hasMore)
            return datum;
        else
            return null;
    }
    
}
