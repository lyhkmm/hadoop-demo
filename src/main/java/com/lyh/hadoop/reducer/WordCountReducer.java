package com.lyh.hadoop.reducer;

/**
 * ＠author linyuanhuang@lyhkmm.com
 * 2019/2/20 10:41
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Text 输入的key的类型
 * IntWritable 输入的value的类型
 * Text 输出的key类型
 * IntWritable 输出的value类型
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * (1,'this is first line')转换成('this',1),('is',1),('first',1),('line',1)后统计各个单词出事的次数
     * @param key 输入map的key
     * @param values 输入map的value
     * @param context 输出
     * @throws IOException 异常
     * @throws InterruptedException 异常
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable((count)));
    }
}
