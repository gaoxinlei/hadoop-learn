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
}
