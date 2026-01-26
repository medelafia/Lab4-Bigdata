package com.lab.mapreduce.ex3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class PrecipitationTotalCounterMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    String header;

    private FloatWritable one = new FloatWritable(1f);
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("data.csv"));
        header = reader.readLine();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        if(line.equals(header) || fields.length < 20) return ;

        String date = fields[1] ;
        String year = date.split("/")[2] ;
        try {
            Float precipitation = Float.parseFloat(fields[10]);

            one.set(precipitation);
            context.write(new Text(year), one);
        }catch(NumberFormatException numberFormatException) {

        }
    }
}
