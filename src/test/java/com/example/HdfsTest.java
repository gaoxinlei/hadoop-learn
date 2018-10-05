package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTest.class);
    @Test
    public void testUpload() throws Exception {
        //1.建立配置。设置namenode和副本数。
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://stack:9000");
        configuration.set("dfs.replication", "1");
        //用户名
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //2.文件系统。
        Path src = new Path("E:\\java\\all_api\\jQueryAPI_1.7.1_CN.chm");
        Path desc = new Path("hdfs://stack:9000/user/hadoop/jquery.chm");
        FileSystem system = desc.getFileSystem(configuration);
        LOGGER.info("文件系统：{}",system);
        system.copyFromLocalFile(src,desc);
        LOGGER.info("成功将源文件:{}拷贝到:{}",src.getName(),desc.getName());
    }

    @Test
    public void testDownload() throws Exception {
        //1.配置类。
        Configuration configuration = new Configuration();
        //2.文件
        Path src = new Path("hdfs://stack:9000/user/hadoop/jquery.chm");
        Path dest = new Path("E:\\java\\jquery.chm");
        FileSystem system = src.getFileSystem(configuration);
        system.copyToLocalFile(false,src,dest,true);
        LOGGER.info("成功将文件:{}下载到:{}",src,dest);
    }

    @Test
    public void testMkDir ()throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration configuration = new Configuration();
        FileSystem system = FileSystem.get(configuration);
        boolean result = system.mkdirs(new Path("hdfs://stack:9000/user/hadoop/test"));
        if(result){
            LOGGER.info("创建文件夹成功");
        }else{
            LOGGER.info("创建文件夹失败");
        }
    }

    /**
     * 源码解析：
     * 创建path时不需要指定前缀。
     * 在path.getFileSystem(conf)时传入一个空的配置，FileSystem会去从conf中get
     * 这个常量FS_DEFAULT_NAME_KEY，即fs.defaultFS,conf会去从两个默认文件
     * core-default.xml和core-site.xml中取值。先设置的为默认值file:///，后从我们的
     * core-site.xml中取出了设置的hdfs://stack:9000/
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Path path = new Path("/user/hadoop/test");
        FileSystem system = path.getFileSystem(new Configuration());
        //第二个参数是递归，若要删除非空文件夹必须指定为true。
        system.delete(path,true);
        LOGGER.info("成功删除了文件夹");
    }

}
