package com.example.comparator;

import com.example.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderComparator extends WritableComparator{
    public OrderComparator(){
        super(OrderBean.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((OrderBean)a).compareTo((OrderBean)b);
    }
}
