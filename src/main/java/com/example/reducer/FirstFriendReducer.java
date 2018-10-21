package com.example.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * 两两共同好友第一次reduce
 */
public class FirstFriendReducer extends Reducer<Text,Text,Text,NullWritable>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(key.toString()).append(":");
        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            sb.append(iterator.next());
            if(iterator.hasNext()){
                sb.append(",");
            }
        }
        context.write(new Text(sb.toString()),NullWritable.get());
    }
}
