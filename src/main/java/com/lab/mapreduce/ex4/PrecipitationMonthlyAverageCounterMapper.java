package com.lab.mapreduce.ex4;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class PrecipitationMonthlyAverageCounterMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
    String header;

    private FloatWritable one = new FloatWritable(1f);
    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, FloatWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("data.csv"));
        header = reader.readLine();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, FloatWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        if(line.equals(header) || fields.length < 20) return ;

        String date = fields[1] ;
        String month = date.split("/")[0] ;
        try {
            float precipitation = Float.parseFloat(fields[10]);

            one.set(precipitation);
            context.write(new IntWritable(Integer.parseInt(month)), one);
        }catch(NumberFormatException numberFormatException) {
            System.out.println(numberFormatException.getMessage() + " in line " + line);
        }
    }
}
