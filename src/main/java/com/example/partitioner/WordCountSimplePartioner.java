package com.example.partitioner;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区类简易抽象。
 */
public class WordCountSimplePartioner extends Partitioner<Text,IntWritable>{
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String param = key.toString();
        if(StringUtils.isEmpty(param)){
            return 0;
        }else{
            if(param.charAt(0)>'m'){
                return 1;
            }
        }
        return 0;
    }
}
