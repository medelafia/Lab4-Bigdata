package com.lab.mapreduce.ex5;

import com.lab.mapreduce.utils.ExtremesTemperatureWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;



public class TemperatureExtremes {


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Job job = Job.getInstance();

        job.setMapperClass(TemperatureExtremesMapper.class);
        job.setReducerClass(TemperatureExtremesReducer.class);
        job.setJarByClass(TemperatureExtremes.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ExtremesTemperatureWritable.class);

        job.addCacheFile(new URI(args[0]+"#data.csv"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1)  ;
    }
}
