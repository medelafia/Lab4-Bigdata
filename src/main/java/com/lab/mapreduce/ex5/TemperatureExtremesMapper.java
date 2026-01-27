package com.lab.mapreduce.ex5;

import com.lab.mapreduce.utils.ExtremesTemperatureWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


public class TemperatureExtremesMapper extends Mapper<LongWritable, Text, Text, ExtremesTemperatureWritable> {
    String header;

    private ExtremesTemperatureWritable output = new ExtremesTemperatureWritable();
    private ExtremesTemperatureWritable output1 = new ExtremesTemperatureWritable();

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, ExtremesTemperatureWritable>.Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("data.csv"));
        header = reader.readLine();
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, ExtremesTemperatureWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        if(line.equals(header) || fields.length < 20) return ;

        String date = fields[1] ;
        String year = date.split("/")[2] ;
        try {
            float minTemp = Float.parseFloat(fields[3]);
            float maxTemp = Float.parseFloat(fields[2]);

            output.setValue(minTemp);
            output.setYear(Integer.parseInt(year));
            output1.setValue(maxTemp);
            output1.setYear(Integer.parseInt(year));

            context.write(new Text("Minimum"), output);
            context.write(new Text("Maximum"), output);

        }catch(NumberFormatException numberFormatException) {
            System.out.println(numberFormatException.getMessage() + " in line " + line);
        }
    }
}
