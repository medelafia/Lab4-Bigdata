package com.lab.mapreduce.ex3;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrecipitationTotalCounterReducer extends Reducer<Text , FloatWritable, Text , FloatWritable> {
    FloatWritable result = new FloatWritable(1f);

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        float sum = 0;
        for(FloatWritable val : values) {
            sum += val.get() ;
        }
        result.set(sum);
        context.write(key, result);
    }
}
