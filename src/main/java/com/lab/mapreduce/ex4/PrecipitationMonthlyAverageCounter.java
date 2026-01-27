package com.lab.mapreduce.ex4;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class PrecipitationMonthlyAverageCounter {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Job job = Job.getInstance();

        job.setMapperClass(PrecipitationMonthlyAverageCounterMapper.class);
        job.setReducerClass(PrecipitationMonthlyAverageCounterReducer.class);
        job.setJarByClass(PrecipitationMonthlyAverageCounter.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.addCacheFile(new URI(args[0]+"#data.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1)  ;
    }
}
