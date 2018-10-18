package com.example.reducer;

import com.example.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean,NullWritable,Text,Text>{
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderReducer.class);
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String orderId = key.getOrderId();
        String productId = key.getProductId();
        LOGGER.info("orderId:{},productId:{}",orderId,productId);
        context.write(new Text(orderId),new Text(key.getPrice()+"--"));
    }
}
