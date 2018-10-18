package com.example.driver;

import com.example.mapper.OrderDistributedCacheMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 分布式缓存测试driver
 * 只能全使用本地路径，或打成jar包在分布式集群或伪分布式集群下使用，否则会提交不上job，原因未知（windows）。
 * 预计在mac下能直接连本机伪分布式集群运行。
 */
public class OrderDistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //基本配置。
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(OrderDistributedCacheDriver.class);
        job.setMapperClass(OrderDistributedCacheMapper.class);
        //缓存。
        job.addCacheFile(new URI("/user/hadoop/cache/pd.txt#pd"));
        //reduce数0.
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
