package com.example.driver;

import com.example.bean.OrderBean;
import com.example.comparator.OrderComparator;
import com.example.mapper.OrderMapper;
import com.example.partitioner.OrderPartitioner;
import com.example.reducer.OrderReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//取每个订单中最高价商品id driver
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
//        configuration.set("mapreduce.framework.name", "yarn");
//        configuration.set("yarn.resourcemanager.hostname", "stack");
        //本地配置.
        System.setProperty("hadoop.home.dir", "E:\\java\\hadoop-2.7.2");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(OrderDriver.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setPartitionerClass(OrderPartitioner.class);

        job.setNumReduceTasks(3);

//        job.setGroupingComparatorClass(OrderComparator.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
