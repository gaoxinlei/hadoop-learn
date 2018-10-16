package com.example.reducer;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 流量统计reducer
 */
public class FlowReducer extends Reducer<Text,FlowBean,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;
        long sumFlow = 0;
        for(FlowBean bean:values){
            upFlow+=bean.getUpFlow();
            downFlow+=bean.getDownFlow();
        }
        sumFlow = upFlow+downFlow;
        FlowBean bean = new FlowBean();
        bean.setUpFlow(upFlow);
        bean.setDownFlow(downFlow);
        bean.setSumFlow(sumFlow);
        context.write(key,new Text(bean.toString()));
    }
}
