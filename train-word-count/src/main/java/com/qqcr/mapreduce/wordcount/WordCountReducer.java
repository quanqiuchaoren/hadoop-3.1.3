package com.qqcr.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    Text keyOut = new Text();
    IntWritable valOut = new IntWritable();

    @Override
    protected void reduce(Text keyIn, Iterable<IntWritable> valuesIn, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // super.reduce(keyIn, valuesIn, context); // 使用idea重写方法时自动生成的代码，注释掉
        int sum = 0;
        for (IntWritable curCount : valuesIn) {
            sum += curCount.get();
        }
        keyOut.set(keyIn);
        valOut.set(sum);
        context.write(keyOut, valOut);
    }
}
