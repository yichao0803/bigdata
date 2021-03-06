package com.zyccx.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1、获取 Jab
        final Configuration config = new Configuration();
        final Job job = Job.getInstance(config);

        // 2、设置 jar 包路径
        job.setJarByClass(WordCountDriver.class);

        // 3、配置 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、配置 map 输出 kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、配置最终的输出 kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、配置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\output"));
        // 7、提交任务
        final boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
