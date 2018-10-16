package com.example.mapper;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 手机流量mapper
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
    @Override
    protected void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {
        String line = text.toString();
        String[] elements = line.split("\t");
        String phone = elements[1];
        long upFlow = Long.parseLong(elements[elements.length - 3]);
        long downFlow = Long.parseLong(elements[elements.length - 2]);
        long sumFlow = upFlow+downFlow;
        FlowBean bean = new FlowBean();
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setSumFlow(sumFlow);
        context.write(new Text(phone),bean);
    }
}
