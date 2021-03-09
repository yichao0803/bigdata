package com.zyccx.shuffleComparable;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * reduce
 */
public class PhoneDataReducer extends Reducer<PhoneDataWritableComparable, Text, Text, PhoneDataWritableComparable> {
    @Override
    protected void reduce(PhoneDataWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            context.write(text, key);
        }
    }
}
