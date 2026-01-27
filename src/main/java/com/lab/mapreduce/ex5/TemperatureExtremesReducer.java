package com.lab.mapreduce.ex5;

import com.lab.mapreduce.utils.ExtremesTemperatureWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TemperatureExtremesReducer extends Reducer<Text , ExtremesTemperatureWritable, Text , ExtremesTemperatureWritable> {
    ExtremesTemperatureWritable result = new ExtremesTemperatureWritable();

    @Override
    protected void reduce(Text key, Iterable<ExtremesTemperatureWritable> values, Reducer<Text, ExtremesTemperatureWritable, Text, ExtremesTemperatureWritable>.Context context) throws IOException, InterruptedException {
        float extremeValue  = 0;
        int extremeYear = 0 ;
        boolean first = true;

        for(ExtremesTemperatureWritable val : values) {
            if(first) {
                extremeYear = val.getYear();
                extremeValue = val.getValue() ;
                first = false;
                continue;
            }
            if(key.toString().equalsIgnoreCase("maximum") && val.getValue() >= extremeValue) {
                extremeYear = val.getYear() ;
                extremeValue = val.getValue() ;
            }else if(key.toString().equalsIgnoreCase("minimum") && val.getValue() <= extremeValue) {
                extremeYear = val.getYear() ;
                extremeValue = val.getValue() ;
            }
        }
        result.setYear(extremeYear);
        result.setValue(extremeValue);

        context.write(key, result);
    }
}
