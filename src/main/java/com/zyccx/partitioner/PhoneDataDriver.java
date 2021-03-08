package com.zyccx.partitioner;


import com.zyccx.serializable.PhoneDataMapper;
import com.zyccx.serializable.PhoneDataReducer;
import com.zyccx.serializable.PhoneDataWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PhoneDataDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 声明 job
        final Job job = Job.getInstance(getConf(), "phone-data-partitioner-job");
        // 设置 jar 类
        job.setJarByClass(getClass());
        // 设置 mapper 和 reducer
        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReducer.class);
        // 设置 mapper 的输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneDataWritable.class);
        // 设置最终 的输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataWritable.class);

        // 设置 partition
        job.setNumReduceTasks(2);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop\\serializable\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\partitioner\\output"));
        // 提交任务
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final int exitCode = ToolRunner.run(new PhoneDataDriver(), args);
        System.exit(exitCode);
    }
}