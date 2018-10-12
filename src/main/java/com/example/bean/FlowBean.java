package com.example.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean>{

    private long upFlow;
    private long downFlow;
    private long sumFlow;
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return this.upFlow+"\t"
                +this.downFlow+"\t"
                +this.sumFlow;
    }

    @Override
    public int compareTo(FlowBean other) {
        return Long.compare(this.upFlow,other.upFlow);
    }
}
