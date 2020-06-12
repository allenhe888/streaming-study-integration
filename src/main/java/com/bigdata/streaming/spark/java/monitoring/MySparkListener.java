package com.bigdata.streaming.spark.java.monitoring;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;

public class MySparkListener extends SparkListener {

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {

    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
    }
}
