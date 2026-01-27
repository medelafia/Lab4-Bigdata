package com.lab.mapreduce.ex4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrecipitationMonthlyAverageCounterReducer extends Reducer<IntWritable , FloatWritable, IntWritable , FloatWritable> {
    FloatWritable result = new FloatWritable(1f);

    @Override
    protected void reduce(IntWritable key, Iterable<FloatWritable> values, Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable>.Context context) throws IOException, InterruptedException {
        float sum = 0;
        int counter = 0 ;
        for(FloatWritable val : values) {
            sum += val.get() ;
            counter ++;
        }
        if(counter > 0) {
            result.set(sum / counter);
            context.write(key, result);
        }
    }
}
