package com.example.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 订单数据使用分布式缓存mapper。
 */
public class OrderDistributedCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    //已经导入到hdfs的缓存信息读取map
    private Map<String,String> productNameMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //将缓存的文件读入map。
        BufferedReader reader = new BufferedReader(new FileReader("pd"));
        String line = null;
        while((line=reader.readLine())!=null){
            String[] pd = line.split("\t");
            productNameMap.put(pd[0],pd[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] order = line.split("\t");
        String orderId = order[0];
        String productId = order[1];
        String number = order[2];
        String productName = productNameMap.get(productId);
        String outputKey = orderId+"\t"+productId+"\t"+productName+"\t"+number;
        context.write(new Text(outputKey),NullWritable.get());
    }
}
