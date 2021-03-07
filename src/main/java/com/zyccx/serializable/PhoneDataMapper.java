package com.zyccx.serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PhoneDataMapper extends Mapper<LongWritable, Text, Text, PhoneDataWritable> {

    public static final Logger LOGGER = LoggerFactory.getLogger(PhoneDataMapper.class);
    private Text outKey = new Text();
    private PhoneDataWritable outValue = new PhoneDataWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String strValue = value.toString();
        final String[] dataArr = strValue.split("\t");

        if (dataArr.length != 7
        ) {
            LOGGER.error("Data length is not enough:{}", strValue);
        }
        Long upLoad = Long.valueOf(dataArr[4]);
        Long downLoad = Long.valueOf(dataArr[5]);

        outKey.set(dataArr[1]);
        outValue.setUpLoad(upLoad);
        outValue.setDownLoad(downLoad);
        outValue.setAllLoad(upLoad + downLoad);

        context.write(outKey, outValue);
    }
}
