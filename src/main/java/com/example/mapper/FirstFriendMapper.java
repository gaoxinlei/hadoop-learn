package com.example.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 两两共同好友案例第一步mapper
 */
public class FirstFriendMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] elements = line.split(":");
        String owner = elements[0];
        String[] friends = elements[1].split(",");
        for(String friend:friends){
            context.write(new Text(friend),new Text(owner));
        }
    }
}
