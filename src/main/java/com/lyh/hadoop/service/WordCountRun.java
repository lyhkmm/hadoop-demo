package com.lyh.hadoop.service;

/**
 * ＠author linyuanhuang@lyhkmm.com
 * 2019/2/20 10:38
 */
import com.lyh.hadoop.Utils.HdfsUtil;
import com.lyh.hadoop.conf.Conf;
import com.lyh.hadoop.map.WordCountMapper;
import com.lyh.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class WordCountRun {
    private static Logger logger = Logger.getLogger("this.class");
    //输入文件路径
    static String inPath = "/hadoop_demo/input/words.txt";
    //输出文件目录
    static String outPath = "/hadoop_demo/output/result/";
    public static void Run() {
        //创建word_input目录
        String folderName = "/hadoop_demo/input";
        HdfsUtil.mkdirFolder(folderName);
        //创建word_input目录
        folderName = "/hadoop_demo/output";
        HdfsUtil.mkdirFolder(folderName);
        //上传文件
        String localPath = "E:\\hadoop\\upload\\";
        String fileName = "words.txt";
        String hdfsPath = "/hadoop_demo/input/";
        HdfsUtil.uploadFile(localPath, fileName, hdfsPath);
        try {
            //执行
            mapReducer();
            //成功后下载文件到本地
            String downName = "part-r-00000";
            String savePath = "E:\\hadoop\\download\\";
            //打印内容
            HdfsUtil.readOutFile(outPath+downName);
            HdfsUtil.getFileFromHadoop(outPath, downName, savePath);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }
    public static void mapReducer() throws Exception{
        //获取连接配置
        Configuration conf = Conf.get();
        //创建一个job实例
        Job job = Job.getInstance(conf,"wordCount");
        //设置job的主类
        job.setJarByClass(WordCountRun.class);
        //设置job的mapper类和reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置Mapper的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置Reduce的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入和输出路径
        FileSystem fs = HdfsUtil.getFiles();
        Path inputPath = new Path(inPath);
        FileInputFormat.addInputPath(job,inputPath);
        Path outputPath = new Path(outPath);
        fs.delete(outputPath,true);
        FileOutputFormat.setOutputPath(job,outputPath);
        job.waitForCompletion(true);

    }
}