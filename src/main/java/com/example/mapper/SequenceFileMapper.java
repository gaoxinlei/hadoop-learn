package com.example.mapper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 整文件读取mapper。
 */
public class SequenceFileMapper extends Mapper<NullWritable,BytesWritable,Text,BytesWritable>{

    private Text key;

    /**
     * 可以不写此方法，将取文件名为key的代码写在map方法中。
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.key = new Text(((FileSplit)context.getInputSplit()).getPath().toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(this.key,value);

    }
}
