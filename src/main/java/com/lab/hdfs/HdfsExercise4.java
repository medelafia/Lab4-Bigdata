package com.lab.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsExercise4 {


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();


        try(
                FileSystem fs = FileSystem.get(conf);
                FSDataInputStream fsDataInputStream = fs.open(new Path("/data/trees.csv"));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        ) {
            String line;
            while((line = bufferedReader.readLine()) != null ) {
                Tree.loadFromLine(line);

                System.out.println( "Planted Year : " + Tree.getYear() + " , Height : " + Tree.getHeight() );
            }
        }catch (IOException e) {
            e.printStackTrace();
        }


    }
}
