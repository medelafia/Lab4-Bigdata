package com.lab.mapreduce.utils;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ExtremesTemperatureWritable implements Writable {
    private float value ;
    private int year ;

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(value);
        dataOutput.writeInt(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.value = dataInput.readFloat();
        this.year = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  year + "\t" + value;
    }
}
