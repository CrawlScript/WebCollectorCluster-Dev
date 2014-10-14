/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cn.edu.hfut.dmic.webcollectorcluster.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author hu
 */
public class Content implements Writable{
    
    public static final String CONTENT_DIR_NAME="content";
    
    public String contentType;
    public byte[] content;

    @Override
    public void write(DataOutput d) throws IOException {
        Text.writeString(d, contentType);
        d.writeInt(content.length);
        d.write(content);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        contentType=Text.readString(di);
        int length=di.readInt();
        content=new byte[length];
        di.readFully(content);
        
    }
    
    
    
}
