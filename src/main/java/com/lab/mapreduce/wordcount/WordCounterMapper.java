package com.lab.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCounterMapper extends Mapper<LongWritable , Text, Text, IntWritable> {
    Text word = new Text();
    IntWritable result = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text , Text,IntWritable>.Context context) throws IOException, InterruptedException {
        word.set(value.toString());
        result.set(1);

        context.write(word , result);

    }
}
