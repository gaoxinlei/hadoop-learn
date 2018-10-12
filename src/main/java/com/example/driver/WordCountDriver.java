package com.example.driver;

import com.example.mapper.WordCountMapper;
import com.example.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * wordcount实例driver
 */
public class WordCountDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountDriver.class);
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "yarn");
		configuration.set("yarn.resourcemanager.hostname", "stack");
        Job job = Job.getInstance(configuration);
        String path = System.getProperty("user.dir");
        LOGGER.info("path:{}",path);
        job.setJarByClass(WordCountDriver.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        LOGGER.info("input path:{},output path:{}",args[0],args[1]);
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
