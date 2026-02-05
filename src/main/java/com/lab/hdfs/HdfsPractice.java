package com.lab.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Scanner;

public class HdfsPractice {


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path("/data/data.txt");
        FileSystem fs = FileSystem.get(conf);
        Scanner scanner = new Scanner(System.in);

        System.out.println("Do you want to delete the file? (y/n)");
        if(scanner.nextLine().toLowerCase().trim().equals("y")) {
            if(fs.exists(path)){
                boolean deleted = fs.delete(path, true);
                if(deleted){
                    System.out.println("Deleted the file");
                }else {
                    System.out.println("Failed to delete the file");
                }
            }else {
                System.out.println("File does not exist");
            }
        }

        FSDataOutputStream out = null;
        if(!fs.exists(path)){
            out = fs.create(path);
        }else {
            out = fs.append(path);
        }

        FSDataInputStream in = fs.open(path);
        String line = scanner.nextLine();

        while(!line.equalsIgnoreCase("exit")) {
            switch(line) {
                case "read":
                    System.out.println("File Content : ");
                    int currentChar = in.read();
                    while((currentChar = in.read()) != -1) {
                        System.out.print((char) currentChar);
                    }
                    in.seek(0);
                    break ;
                case "write":
                    System.out.println("Write to file : \nPlease enter content to be written:");
                    out.writeChars(scanner.nextLine());
                    break;
            }

            System.out.println("Please Select:");
            System.out.println("Read");
            System.out.println("Write");
            System.out.println("Exit");

            System.out.println(">>");
            line = scanner.nextLine();
        } ;


        in.close();
        out.close();
        fs.close();
    }
}
