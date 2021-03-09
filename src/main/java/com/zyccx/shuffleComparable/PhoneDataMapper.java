package com.zyccx.shuffleComparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 输入：读取一行流量信息
 * 提取：上行流量，下行流量，计算总流量
 * 输出：流量信息，手机号
 *
 */
public class PhoneDataMapper extends Mapper<LongWritable, Text,PhoneDataWritableComparable,Text> {

    PhoneDataWritableComparable outKey=new PhoneDataWritableComparable();
    Text outValue=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1	13523234545	ip	www.baidu.com	1112	56	200
        // 按 \t 分割数据
        final String[] split = value.toString().split("\t");
        // 提取信息
        outKey.setUpLoad(Long.valueOf(split[4]));
        outKey.setDownLoad(Long.valueOf(split[5]));
        outKey.setAllLoad();
        outValue.set(split[1]);
        // 输出 kv
        context.write(outKey,outValue);
    }
}
