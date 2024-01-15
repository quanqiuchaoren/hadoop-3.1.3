package com.qqcr.mapreduce.wordcount;

import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入路径
        String inputPathString;
        // 输出路径
        String outPathString;
        if (SystemUtils.IS_OS_WINDOWS) { // SystemUtils是commons-lang3中的api
            System.out.println("当前操作系统是 Windows");
            // linux中直接指定输入和输出的文件夹
            inputPathString = "D:\\hadoop\\test_data\\input2";
            outPathString = "D:\\hadoop\\test_data\\output3";
        } else if (SystemUtils.IS_OS_LINUX) {
            /*
            在linux系统上，使用以下命令运行本代码
            hadoop jar ./myjar/train-word-count-1.0-SNAPSHOT-jar-with-dependencies.jar com.qqcr.mapreduce.wordcount.WordCountDriver /input  /output5
            jar包路径：./myjar/train-word-count-1.0-SNAPSHOT-jar-with-dependencies.jar，需要在本地打包完成后，上传到linux系统中。
            /input：其中的/input是大数据中的路径，可以在大数据的web网页查看（http://hadoop102:9870/explorer.html#/），或者使用hdfs命令查看
            /output5：此目录是程序的输出目录，不能存储，程序会自己创建。输出会自动在hdfs中生成。

             */
            // linux系统从参数中读取
            System.out.println("当前操作系统是 Linux");
            inputPathString = args[0];
            outPathString = args[1];
        } else if (SystemUtils.IS_OS_MAC) {
            System.out.println("当前操作系统是 macOS");
            throw new RuntimeException("not supported os macOS");
        } else if (SystemUtils.IS_OS_SOLARIS) {
            System.out.println("当前操作系统是 Solaris");
            throw new RuntimeException("not supported os Solaris");
        } else {
            System.out.println("无法确定当前操作系统");
            throw new RuntimeException("unknown os");
        }

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(inputPathString));
        FileOutputFormat.setOutputPath(job, new Path(outPathString));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
