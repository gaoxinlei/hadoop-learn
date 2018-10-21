package com.example.inputformat;

import com.example.reader.WholeRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 一次读一个文件
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable,BytesWritable>{
    /**
     * 文件不可分割
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * 创建reader
     * @param split
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader reader = new WholeRecordReader();
        //一定要调用初始化方法，类似构造器（参考servlet），将重要的对象传递给reader.
        reader.initialize(split,context);
        return reader;
    }
}
