package com.zyccx.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * WordCountMapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text outKey = new Text();
    IntWritable outValue = new IntWritable(1);

    /**
     * map
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 以空格拆分一行，单词
        final String[] words = value.toString().split(" ");
        // 遍历单词
        for (String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}
