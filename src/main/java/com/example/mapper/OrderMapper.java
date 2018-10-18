package com.example.mapper;

import com.example.bean.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable>{
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderMapper.class);
    OrderBean bean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] elements = line.split("\t");
        String orderId = elements[0];
        String productId = elements[1];
        double price = Double.parseDouble(elements[2]);
        bean.setOrderId(orderId);
        bean.setProductId(productId);
        bean.setPrice(price);
        LOGGER.info("map key:{}",bean);
        context.write(bean,NullWritable.get());
    }
}
