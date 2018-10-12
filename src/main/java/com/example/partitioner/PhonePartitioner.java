package com.example.partitioner;

import com.example.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class PhonePartitioner implements Partitioner<Text,FlowBean>{
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        String phone = key.toString();
        if(phone.startsWith("136")){
            return 0;
        }else if (phone.startsWith("137")){
            return 1;
        }else if (phone.startsWith("138")){
            return 2;
        }
        return 3;
    }

    @Override
    public void configure(JobConf job) {

    }
}
