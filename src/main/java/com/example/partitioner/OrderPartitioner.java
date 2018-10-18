package com.example.partitioner;

import com.example.bean.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Objects;

public class OrderPartitioner extends Partitioner<OrderBean,NullWritable>{

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return Objects.hash(orderBean.getOrderId())%numPartitions;
    }
}
