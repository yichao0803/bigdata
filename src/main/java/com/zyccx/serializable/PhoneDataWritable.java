package com.zyccx.serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneDataWritable implements Writable {

    private Long upLoad;
    private Long downLoad;
    private Long allLoad;

    public PhoneDataWritable() {
        super();
    }

    public PhoneDataWritable(Long upLoad, Long downLoad) {
        super();
        this.upLoad = upLoad;
        this.downLoad = downLoad;
    }

    public Long getUpLoad() {
        return upLoad;
    }

    public void setUpLoad(Long upLoad) {
        this.upLoad = upLoad;

    }

    public Long getDownLoad() {
        return downLoad;
    }

    public void setDownLoad(Long downLoad) {
        this.downLoad = downLoad;

    }

    public Long getAllLoad() {
        return allLoad;
    }

    public void setAllLoad(Long allLoad) {
        this.allLoad = allLoad;
    }
    public void setAllLoad() {
        this.allLoad = upLoad + downLoad;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upLoad);
        out.writeLong(downLoad);
        out.writeLong(allLoad);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upLoad = in.readLong();
        this.downLoad = in.readLong();
        this.allLoad = in.readLong();
    }

    @Override
    public String toString() {
        return this.upLoad +
                "\t" + this.downLoad +
                "\t" + this.allLoad;
    }
}
