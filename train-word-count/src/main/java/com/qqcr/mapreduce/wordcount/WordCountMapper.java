package com.qqcr.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text keyOut = new Text();
    IntWritable valOut = new IntWritable(1);

    @Override
    protected void map(LongWritable keyIn, Text valIn, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // super.map(keyIn, valIn, context); // 这一行是自动生成的代码，注释掉
        String[] words = valIn.toString().split(" ");// 通过空格分隔这一行中的单词
        for (String word : words) { // 这里不能用stream，不然会要求处理已经在方法中抛出的IOException和InterruptedException
            keyOut.set(word);
            context.write(keyOut, valOut);
        }
    }
}
