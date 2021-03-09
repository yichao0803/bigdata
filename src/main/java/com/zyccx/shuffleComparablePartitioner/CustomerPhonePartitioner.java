package com.zyccx.shuffleComparablePartitioner;

import com.zyccx.shuffleComparable.PhoneDataWritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class CustomerPhonePartitioner extends Partitioner<PhoneDataWritableComparable, Text> {
    @Override
    public int getPartition(PhoneDataWritableComparable phoneDataWritableComparable, Text text, int numPartitions) {
        String phone = text.toString();
        String subPhone = phone.substring(0, 3);
        Integer partitionNum = 4;

        if ("135".equals(subPhone)) {
            partitionNum = 0;
        } else if ("136".equals(subPhone)) {
            partitionNum = 1;
        } else if ("137".equals(subPhone)) {
            partitionNum = 2;
        } else if ("139".equals(subPhone)) {
            partitionNum = 3;
        }
        return partitionNum;
    }
}
