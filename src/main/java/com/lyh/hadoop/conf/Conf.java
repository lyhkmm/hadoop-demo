package com.lyh.hadoop.conf;
/**
 * ＠author linyuanhuang@lyhkmm.com
 * 2019/2/19 17:01
 */
import org.apache.hadoop.conf.Configuration;

public class Conf {
    public static Configuration get (){
        //hdfs的链接地址
        String hdfsUrl = "hdfs://hadoop1:9000";
        //hdfs的名字
        String hdfsName = "fs.defaultFS";
        //jar包文位置(上一个步骤获得的jar路径)
        String jarPath = "E:\\OneDrive\\study\\hadoop-demo\\out\\artifacts\\hadoop_demo_jar\\hadoop-demo.jar";

        Configuration conf = new Configuration();
        conf.set(hdfsName,hdfsUrl);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.job.jar",jarPath);
        return conf;
    }
}