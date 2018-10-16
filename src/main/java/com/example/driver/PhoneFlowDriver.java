package com.example.driver;

import com.example.bean.FlowBean;
import com.example.mapper.FlowMapper;
import com.example.partitioner.PhonePartitioner;
import com.example.reducer.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 统计流量驱动类
 */
public class PhoneFlowDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoneFlowDriver.class);
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "stack");
        Job job = Job.getInstance(configuration);
        String path = System.getProperty("user.dir");
        LOGGER.info("path:{}",path);

        job.setJarByClass(PhoneFlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //分区汇总需要保证map与reduce输出同类。
//        job.setCombinerClass(FlowReducer.class);
        job.setPartitionerClass(PhonePartitioner.class);
        job.setNumReduceTasks(5);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        LOGGER.info("input path:{},output path:{}",args[0],args[1]);
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
