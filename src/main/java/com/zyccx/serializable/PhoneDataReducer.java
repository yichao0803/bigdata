package com.zyccx.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneDataReducer extends Reducer<Text, PhoneDataWritable, Text, PhoneDataWritable> {

    PhoneDataWritable outValue = new PhoneDataWritable();

    @Override
    protected void reduce(Text key, Iterable<PhoneDataWritable> values, Context context) throws IOException, InterruptedException {
        Long sumUpload = 0L;
        Long sumDownLoad = 0L;
        // 遍历每次访问流量，并统计
        for (PhoneDataWritable phoneDataWritable : values) {
            sumUpload += phoneDataWritable.getUpLoad();
            sumDownLoad += phoneDataWritable.getDownLoad();
        }

        outValue.setUpLoad(sumUpload);
        outValue.setDownLoad(sumDownLoad);
        outValue.setAllLoad(sumUpload + sumDownLoad);

        context.write(key, outValue);
    }
}
