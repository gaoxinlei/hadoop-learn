package com.example.partitioner;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        String phone = key.toString();
        if(phone.startsWith("136")){
            return 1;
        }else if (phone.startsWith("137")){
            return 2;
        }else if (phone.startsWith("138")){
            return 3;
        }else if(phone.startsWith("139")){
            return 4;
        }
        return 0;
    }

}
