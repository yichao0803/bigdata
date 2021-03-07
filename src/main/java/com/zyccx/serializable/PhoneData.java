package com.zyccx.serializable;

public class PhoneData implements Comparable<PhoneData> {
    private Long upLoad;
    private Long downLoad;


    public PhoneData() {
        this.upLoad = 0L;
        this.downLoad = 0L;

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

    public Long getAllData() {
        return downLoad + upLoad;
    }


    @Override
    public int compareTo(PhoneData o) {
        Long thisValue = getAllData();
        Long thatValue = o.getAllData();
        return (thisValue < thatValue ? -1 : (thisValue.equals(thatValue) ? 0 : 1));
    }

    @Override
    public String toString() {
        return "\t" + upLoad +
                "\t" + downLoad +
                "\t" + getAllData();
    }
}
