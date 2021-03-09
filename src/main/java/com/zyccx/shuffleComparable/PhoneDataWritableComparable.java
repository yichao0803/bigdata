package com.zyccx.shuffleComparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 总流量，倒序 排序
 * 总流量一样的，按上行流量正序 排序
 */

public class PhoneDataWritableComparable implements WritableComparable<PhoneDataWritableComparable> {

    private Long upLoad;
    private Long downLoad;
    private Long allLoad;

    public PhoneDataWritableComparable() {
        super();
    }

    public PhoneDataWritableComparable(Long upLoad, Long downLoad) {
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

    /**
     * 总流量，倒序 排序
     * 总流量一样的，按上行流量正序 排序
     *
     * @param o
     * @return
     */
    @Override
    public int compareTo(PhoneDataWritableComparable o) {
//        Long thisValue = this.allLoad;
//        Long thatValue = o.getAllLoad();
//        return (thisValue < thatValue ? -1 : (thisValue.equals(thatValue) ? 0 : 1));
        // 总流量，倒序 排序
        if (allLoad > o.getAllLoad()) {
            return -1;
        } else if (allLoad < o.getAllLoad()) {
            return 1;
        } else {
            // 总流量一样的，按上行流量正序 排序
            if (upLoad > o.getUpLoad()) {
                return 1;
            } else if (upLoad < o.getDownLoad()) {
                return -1;
            } else {
                return 0;
            }
        }
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
