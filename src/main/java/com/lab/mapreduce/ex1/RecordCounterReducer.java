package com.lab.mapreduce.ex1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecordCounterReducer extends Reducer<Text , IntWritable , Text , IntWritable> {
    IntWritable result = new IntWritable(1);

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val : values) {
            sum += val.get() ;
        }
        result.set(sum);
        context.write(key, result);
    }
}
