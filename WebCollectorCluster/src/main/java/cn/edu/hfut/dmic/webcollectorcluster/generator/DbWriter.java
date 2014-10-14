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


import cn.edu.hfut.dmic.webcollectorcluster.model.AvroModel;
import cn.edu.hfut.dmic.webcollectorcluster.model.CrawlDatum;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 写Avro文件的Writer
 * @author hu
 * @param <T> 待写入数据的数据类型
 */
public class DbWriter<T> {
    
    
    
    private DataFileWriter<T> dataFileWriter;
 
    private Class<T> type;

    /**
     * 构造一个向avro文件中写入指定类型数据的Writer
     * @param type 指定的数据类型
     * @param dbfile avro文件
     * @param append 是否追加
     * @throws IOException
     */
    public DbWriter(Class<T> type,Path path,Configuration config,FileSystem fs,boolean append) throws IOException{
        this.type=type;
        DatumWriter<T> datumWriter = new ReflectDatumWriter<T>(type);
        dataFileWriter = new DataFileWriter<T>(datumWriter);
        OutputStream os=fs.create(path);
        if(!append){
            //if(!dbfile.getParentFile().exists()){
             //   dbfile.getParentFile().mkdirs();
           // }
            dataFileWriter.create(AvroModel.getSchema(type), os);
            
        }else{
            dataFileWriter.appendTo(new FsInput(path, config),os);
        }
    }
    
    
    
    /**
     * 刷新该Writer的缓冲
     * @throws IOException
     */
    public void flush() throws IOException{
        dataFileWriter.flush();
    }
    
    /**
     * 写入数据
     * @param data 要写入的数据
     * @throws IOException
     */
    public void write(T data) throws IOException{
        dataFileWriter.append(data);
    }
    
    /**
     * 关闭该Writer
     * @throws IOException
     */
    public void close() throws IOException{
        dataFileWriter.close();
    }
    
    
    public static void main(String[] args) throws IOException{
        Configuration conf=new Configuration();
        Path path=new Path("/home/hu/data/cluster/crawldb/current/test.avro");
        FileSystem fs=path.getFileSystem(conf);
        DbWriter<CrawlDatum> writer=new DbWriter<CrawlDatum>(CrawlDatum.class,path,conf,fs,false);
        CrawlDatum datum=new CrawlDatum();
        datum.setUrl("http://www.baidu.com");
        datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
        datum.setFetchTime(System.currentTimeMillis());
        writer.write(datum);
        writer.close();
    }
}
