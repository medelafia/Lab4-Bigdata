package com.lab.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

public class HdfsExercise {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path pathIn = new Path("file:///home/hadoop/data/data.csv");
        Path pathOut = new Path("/data/trees.csv");

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(pathIn);
        FSDataOutputStream out = fs.create(pathOut);

        int aChar ;
        while((aChar = in.read()) != -1){
            out.writeChar(aChar);
        }

        in.seek(0);

        out.close();
        in.close();
        fs.close();
    }
}
