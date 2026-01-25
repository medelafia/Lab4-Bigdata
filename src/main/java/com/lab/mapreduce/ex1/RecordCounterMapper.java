package com.lab.mapreduce.ex1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

class Record {
    public Float tempmax ;
    public Float  tempmin;
    public Float temp ;
    public Float feelslikemax ;
    public Float feelslikemin ;
    public Float feelslike ;
    public Float dew ;
    public Float humidity ;
    public Float precip ;
    public Float precipprob ;
    public Float precipcover ;
    public String preciptype ;
    public Float windspeed ;
    public Float winddir ;
    public Float sealevelpressure ;
    public Float cloudcover ;
    public Float  visibility ;
    public Float conditions ;


    public Record(Float tempmax, Float tempmin, Float temp, Float feelslikemax, Float feelslikemin, Float feelslike, Float dew, Float humidity, Float precip, Float precipprob, Float precipcover, String preciptype, Float windspeed, Float winddir, Float sealevelpressure, Float cloudcover, Float visibility, Float conditions) {
        this.tempmax = tempmax;
        this.tempmin = tempmin;
        this.temp = temp;
        this.feelslikemax = feelslikemax;
        this.feelslikemin = feelslikemin;
        this.feelslike = feelslike;
        this.dew = dew;
        this.humidity = humidity;
        this.precip = precip;
        this.precipprob = precipprob;
        this.precipcover = precipcover;
        this.preciptype = preciptype;
        this.windspeed = windspeed;
        this.winddir = winddir;
        this.sealevelpressure = sealevelpressure;
        this.cloudcover = cloudcover;
        this.visibility = visibility;
        this.conditions = conditions;
    }
}
public class RecordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    String header;

    private IntWritable one = new IntWritable(1);
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("file.csv"));
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
