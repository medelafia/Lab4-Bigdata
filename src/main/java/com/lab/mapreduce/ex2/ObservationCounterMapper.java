package com.lab.mapreduce.ex2;

import com.lab.mapreduce.utils.Record;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class ObservationCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    String header;

    private IntWritable one = new IntWritable(1);
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("file.csv"));
        header = reader.readLine();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        if(line.equals(header) || fields.length < 20) return ;

        String date = fields[1] ;
        String year = date.split("/")[2] ;

        context.write(new Text(year), one);
    }
}
