package com.zyccx.wc.v2;

import com.zyccx.wc.v1.WordCountMapper;
import com.zyccx.wc.v1.WordCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCountDriver extends Configured implements Tool {

    Logger logger = LoggerFactory.getLogger(WordCountDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        // 获取系统参数
        if (args.length != 2) {
            logger.error("Usage: %s [generic option] <input> <output> \n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        // 1、声明 Job和配置Job
        final Job job = new Job(getConf(), "Word Count Tool");

        // 2、设置 jar
        job.setJarByClass(getClass());

        // 3、设置 mapper 和 reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4、设置 mapper 的输出 kv 类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终的输出 kv 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入和输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7、提交任务
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountDriver(), args);
        System.exit(exitCode);
    }
}
