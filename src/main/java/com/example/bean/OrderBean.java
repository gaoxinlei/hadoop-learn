package com.example.bean;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 订单案例bean。
 */
@Data
public class OrderBean implements WritableComparable<OrderBean>{
    private String orderId;//订单id
    private String productId;//商品id
    private Double price;//价格
    //如果实际使用到排序，必须有空构造器且初始化所有字段。
    public OrderBean(){
        super();
    }
    @Override
    public int compareTo(OrderBean other) {
        int result = this.orderId.compareTo(other.orderId);
        if(result!=0){
            return result;
        }else{
            result = this.price.compareTo(other.price);
        }
        if(result!=0){
            return result;
        }else{
            return this.productId.compareTo(other.productId);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();
    }
    @Override
    public String toString(){
        return this.orderId+"\t"
                +this.productId+"\t"
                +this.price;
    }

}
