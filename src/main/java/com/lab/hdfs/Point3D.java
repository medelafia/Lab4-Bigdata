package com.lab.hdfs;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point3D implements WritableComparable<Point3D> {
    private double x ;
    private double y ;
    private double z ;

    public Point3D() {

    }
    public Point3D(double x, double y, double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getZ() {
        return z;
    }

    public void setZ(double z) {
        this.z = z;
    }

    public float distance() {
        return (float) Math.sqrt( this.x * this.x + this.y * this.y + this.z * this.z ) ;
    }

    @Override
    public int compareTo(Point3D o) {
        float distance = this.distance();
        float objectDistance = o.distance();
        return Float.compare(distance, objectDistance) ;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(x);
        dataOutput.writeDouble(y);
        dataOutput.writeDouble(z);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.x = dataInput.readDouble();
        this.y = dataInput.readDouble();
        this.z = dataInput.readDouble();
    }
}
