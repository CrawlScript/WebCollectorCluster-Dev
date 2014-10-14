/*
 * Copyright (C) 2014 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollectorcluster.generator;


import cn.edu.hfut.dmic.webcollectorcluster.util.CrawlerConfiguration;
import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 读Avro文件的Reader
 * @author hu
 * @param <T> 待读取数据的数据类型
 */
public class DbReader<T> {

    Class<T> type;
    Iterator<T> iterator;
    DataFileReader<T> dataFileReader;

    /**
     * 构造一个从avro文件中读取指定类型数据的Reader
     * @param type 指定的数据类型
     * @param dbfile 待读取的avro文件
     * @throws IOException
     */
    public DbReader(Class<T> type,Path path,Configuration config,FileSystem fs) throws IOException {
        this.type=type;
        DatumReader<T> datumReader = new ReflectDatumReader<T>(type);
        FsInput fsinput=new FsInput(path, config);
        dataFileReader = new DataFileReader<T>(fsinput, datumReader);
        iterator = dataFileReader.iterator();
    }

    

    /**
     * 读取下一条数据，在文件结束时调用该方法会出错，所以在调用readNext方法前需要使
     * 用hasNext方法来判断文件是否结束
     * @return 下一条数据
     */
    public T readNext() {
        return iterator.next();
    }

    /**
     * 判断是否已读取到avro文件结尾
     * @return 是否已读取到avro文件结尾
     */
    public boolean hasNext(){
        return iterator.hasNext();
    }
    
    /**
     * 关闭该Reader
     * @throws IOException
     */
    public void close() throws IOException {
        dataFileReader.close();
    }

    
    public static void main(String[] args) throws IOException{
        
        Configuration conf=CrawlerConfiguration.create();
        Path path=new Path("/home/hu/data/cluster/crawldb/current/test.avro");
        FileSystem fs=path.getFileSystem(conf);
        
        
       
        DbReader<CrawlDatum> reader=new DbReader<CrawlDatum>(CrawlDatum.class,path,conf,fs);
        int sum=0;
        int sum_fetched=0;
        int sum_unfetched=0;
        
        
        CrawlDatum crawldatum=null;

        System.out.println("start read:");
        while(reader.hasNext()){
            crawldatum=reader.readNext();
            System.out.println(crawldatum.getUrl());
            sum++;
            switch(crawldatum.getStatus()){
                case CrawlDatum.STATUS_DB_FETCHED:
                    sum_fetched++;
                    break;
                case CrawlDatum.STATUS_DB_UNFETCHED:
                    sum_unfetched++;
                    break;
                    
            }
            
         
        }
        reader.close();
       
        
    }
    
}
