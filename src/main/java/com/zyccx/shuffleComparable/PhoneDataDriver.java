package com.zyccx.shuffleComparable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 */
public class PhoneDataDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 声明 job
        final Job job = Job.getInstance(getConf());
        // 设置 jar class 路径
        job.setJarByClass(getClass());
        // 设置 mapper 、reducer class
        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReducer.class);
        // 设置 mapper 输出 kv
        job.setMapOutputKeyClass(PhoneDataWritableComparable.class);
        job.setMapOutputValueClass(Text.class);
        // 设置 最终的输出 kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataWritableComparable.class);
        // 设置 输入、输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop\\serializable\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\output-shuffle-comparable-2"));
        // 提交 job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final int run = ToolRunner.run(new PhoneDataDriver(), args);

        System.exit(run);

    }
}
