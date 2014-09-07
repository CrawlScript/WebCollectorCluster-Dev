/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollector2.crawl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hu
 */
public class CrawlDatum implements Writable{
    
    public static final String GENERATE_DIR_NAME="crawl_generate";
    public static final String FETCH_DIR_NAME="crawl_fetch";
    public static final String PARSE_DIR_NAME="crawl_parse";
    
    public static final int STATUS_DB_UNFETCHED=1;
    public static final int STATUS_DB_FETCHED=2;
    
    
    public int status;
    public long fetchTime;
    public int retries;

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(status);
        d.writeLong(fetchTime);
        d.writeInt(retries);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        status=di.readInt();
        fetchTime=di.readLong();
        retries=di.readInt();
    }
    
}
