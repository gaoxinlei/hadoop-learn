package com.example.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeRecordReader extends RecordReader<NullWritable,BytesWritable>{
    private FileSplit split ;//切片。
    private Configuration configuration;
    private BytesWritable value = new BytesWritable();
    private boolean processd;//步骤
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processd){
            //未处理，仅处理一次。
            //取流。
            Path path = split.getPath();
            FileSystem system = path.getFileSystem(configuration);
            FSDataInputStream fis = system.open(path);
            byte[] buf = new byte[(int) split.getLength()];
            IOUtils.readFully(fis,buf,0,buf.length);
            IOUtils.closeStream(fis);
            value.set(buf,0,buf.length);
            processd = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processd?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}
