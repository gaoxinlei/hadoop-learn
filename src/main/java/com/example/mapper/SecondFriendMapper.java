package com.example.mapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 查找共同好友第二步mapper。
 */
public class SecondFriendMapper extends Mapper<LongWritable,Text,Text,Text>{
    /**
     * 读取一行文本，为本人：有我好友的列表 格式
     * 写出的为任意两个有本人好友的人的组合键，值为本人名。
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] elements = line.split(":");
        String name = elements[0];
        String[] friends = elements[1].split(",");
        Set<String> pairs = makeFriendPair(friends);
        for(String pair:pairs){
            context.write(new Text(pair),new Text(name));
        }
    }

    private Set<String> makeFriendPair(String[] friends) {
        List<String> allFriends = new ArrayList<>(new HashSet<>(Arrays.asList(friends)));
        if(CollectionUtils.isEmpty(allFriends)||allFriends.size()==1){
            //只在一个人的好友列表中，无法生成pair
            return new HashSet<>();//返回空的set，不会进入for循环。
        }
        Set<String> result = new HashSet<>();
        for(int i=0;i<allFriends.size();i++){
            for(int j=i+1;j<allFriends.size();j++){
                String pair = allFriends.get(i)+"->"+allFriends.get(j);
                result.add(pair);
            }
        }
        return result;
    }
}
