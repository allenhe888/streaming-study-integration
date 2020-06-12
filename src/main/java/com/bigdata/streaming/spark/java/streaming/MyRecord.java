package com.bigdata.streaming.spark.java.streaming;

import java.io.Serializable;

public class MyRecord implements Serializable {

    String orgId;
    String modelId;
    String assetId;
    String pointId;
    long time;
    double value;
    int dq;

    public MyRecord() {}

    public MyRecord(String orgId, String modelId, String assetId, String pointId, long time, double value, int dq) {
        this.orgId = orgId;
        this.modelId = modelId;
        this.assetId = assetId;
        this.pointId = pointId;
        this.time = time;
        this.value = value;
        this.dq = dq;
    }

    @Override
    public String toString() {
        return "MyRecord{" +
                "orgId='" + orgId + '\'' +
                ", modelId='" + modelId + '\'' +
                ", assetId='" + assetId + '\'' +
                ", pointId='" + pointId + '\'' +
                ", time=" + time +
                ", value=" + value +
                ", dq=" + dq +
                '}';
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getPointId() {
        return pointId;
    }

    public void setPointId(String pointId) {
        this.pointId = pointId;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int getDq() {
        return dq;
    }

    public void setDq(int dq) {
        this.dq = dq;
    }
}
