package com.lyh.hadoop.map;

/**
 * ＠author linyuanhuang@lyhkmm.com
 * 2019/2/20 10:38
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * LongWritable 行号 类型
 * Text 输入的value 类型
 * Text 输出的key 类型
 * IntWritable 输出的vale类型
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    /**
     * 将文档按照行号转换成map，如(1,'this is first line'),(2,'this is second line')
     * 再将(1,'this is first line')转换成('this',1),('is',1),('first',1),('line',1)
     * @param key 行号
     * @param value 第一行的内容 如  this is first line
     * @param context 输出
     * @throws IOException 异常
     * @throws InterruptedException 异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //规则：以中英文逗号、空格（一个或多个）分割字符串
        String regex = ",|，|\\.|\\s+";
        String[] words = line.split(regex);
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}