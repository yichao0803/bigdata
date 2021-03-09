package com.zyccx.shuffleComparablePartitioner;

import com.zyccx.shuffleComparable.PhoneDataMapper;
import com.zyccx.shuffleComparable.PhoneDataReducer;
import com.zyccx.shuffleComparable.PhoneDataWritableComparable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class PhoneDataDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 声明 Job
        final Job job = Job.getInstance(getConf());
        // 配置驱动 jar 类
        job.setJarByClass(getClass());
        // 配置 mapper 、reducer
        job.setMapperClass(PhoneDataMapper.class);
        job.setReducerClass(PhoneDataReducer.class);
        // 配置 map 的输出 kv 类型
        job.setMapOutputKeyClass(PhoneDataWritableComparable.class);
        job.setMapOutputValueClass(Text.class);
        // 配置 自定义分区、reduce 任务数量（分区）
        job.setPartitionerClass(CustomerPhonePartitioner.class);
        job.setNumReduceTasks(5);
        // 配置 任务最终输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneDataWritableComparable.class);
        // 配置 输入路径、输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\hadoop\\serializable\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop\\output-shuffle-comparable-partitioner-2"));
        // 提交任务
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        final int run = ToolRunner.run(new PhoneDataDriver(), args);
        System.exit(run);
    }
}
