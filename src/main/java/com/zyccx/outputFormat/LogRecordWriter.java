package com.zyccx.outputFormat;


import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream fsOutZyc;
    private FSDataOutputStream fsOutOther;

    public LogRecordWriter(TaskAttemptContext job) {

        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            fsOutZyc = fs.create(new Path("E:\\hadoop\\output-format\\zyc.log"));
            fsOutOther = fs.create(new Path("E:\\hadoop\\output-format\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String s = key.toString() + "\n";
        if (key.toString().contains("zyc")) {
            fsOutZyc.writeBytes(s);
        } else {
            fsOutOther.writeBytes(s);
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fsOutZyc);
        IOUtils.closeStream(fsOutOther);
    }
}
