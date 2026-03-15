package com.lab.hdfs;


public abstract class Tree {

    private static String[] fields ;

    public static void loadFromLine(String line) {
        fields = line.split(";");
    }

    public static int getYear() throws NumberFormatException {
        return Integer.parseInt(fields[5]);
    }

    public static double getHeight() throws NumberFormatException  {
        return Double.parseDouble(fields[6]);
    }

}
