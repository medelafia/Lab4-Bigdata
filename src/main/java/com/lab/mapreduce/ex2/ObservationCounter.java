package com.lab.mapreduce.ex2;

import com.lab.mapreduce.ex1.RecordCounterMapper;
import com.lab.mapreduce.ex1.RecordCounterReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class ObservationCounter {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Job job = Job.getInstance();

        job.setMapperClass(ObservationCounterMapper.class);
        job.setReducerClass(ObservationCounterReducer.class);
        job.setJarByClass(ObservationCounter.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.addCacheFile(new URI(args[0]+"#file.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1)  ;
    }
}
