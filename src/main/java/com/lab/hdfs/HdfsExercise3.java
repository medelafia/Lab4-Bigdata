package com.lab.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsExercise3 {
    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path("/data/trees.csv") ;
        try(
                FSDataInputStream inputStream = fileSystem.open(path);
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        ) {
            int count = 0;
            while (bufferedReader.readLine() != null) {
                count++;
            }
            System.out.println("The number of lines : " + count);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            fileSystem.close();
        }
    }
}
