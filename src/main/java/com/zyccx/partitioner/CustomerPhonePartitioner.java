package com.zyccx.partitioner;

import com.zyccx.serializable.PhoneDataWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomerPhonePartitioner extends Partitioner<Text, PhoneDataWritable> {
    @Override
    public int getPartition(Text text, PhoneDataWritable phoneDataWritable, int numPartitions) {
        String key = text.toString();
        String subKey = key.substring(0, 3);
        Integer parNum = 4;
        if ("134".equals(subKey)) {
            parNum = 0;
        } else if ("135".equals(subKey)) {
            parNum = 1;
        } else if ("136".equals(subKey)) {
            parNum = 2;
        } else if ("137".equals(subKey)) {
            parNum = 3;
        }
        return parNum;
    }
}
