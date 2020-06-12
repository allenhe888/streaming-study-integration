package com.bigdata.streaming.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

public class JsonDeserializer implements Deserializer<JSONObject> {
    private String encoding = "UTF8";
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
    @Override
    public JSONObject deserialize(String topic, byte[] data) {
        try {
            if (data == null)
                return null;
            else
                return JSON.parseObject(new String(data,encoding));
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to JSONObject due to " + e.getMessage());
        }
    }

    @Override
    public void close() {
    }
}
