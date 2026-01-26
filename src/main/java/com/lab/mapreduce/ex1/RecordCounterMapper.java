package com.lab.mapreduce.ex1;

import com.lab.mapreduce.utils.Record;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class RecordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    String header;

    private IntWritable one = new IntWritable(1);
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("data.csv"));
        header = reader.readLine();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        Text outputKey = new Text(fields[1]);

        if(line.equals(header) || fields.length < 20) return ;

        Record record = new Record(Float.parseFloat(fields[2]) , Float.parseFloat(fields[3]), Float.parseFloat(fields[4]) ,Float.parseFloat(fields[5]) , Float.parseFloat(fields[6]), Float.parseFloat(fields[7]), Float.parseFloat(fields[8]), Float.parseFloat(fields[9]) , Float.parseFloat(fields[10]) , Float.parseFloat(fields[11]) , Float.parseFloat(fields[12]) , fields[13] , Float.parseFloat(fields[14]),Float.parseFloat(fields[15]), Float.parseFloat(fields[16]),Float.parseFloat(fields[17]), Float.parseFloat(fields[18]),Float.parseFloat(fields[19]) ) ;
        if(record.humidity <= 100 && record.visibility >= 0 && record.tempmax > record.tempmin && record.sealevelpressure >= 870 && record.sealevelpressure <= 1085) {
            context.write( outputKey , one );
        }
    }
}
