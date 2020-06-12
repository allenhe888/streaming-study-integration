package com.bigdata.streaming.streamsets.common;


import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.sdk.RecordCreator;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SdcCommHelper {

    private static String orgId = "o15453595539601";
    private static String modelId = "myModelId";
    private static String assetId = "myAssetId";
    private static String pointId = "myPointId";
    private static String startEventTime = "2019-01-01T00:01:59";
    private static SimpleDateFormat recordGenFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    private static long startEventTimeMillis ;

    static {
        try {
            startEventTimeMillis = recordGenFormat.parse(startEventTime).getTime();
        } catch (ParseException e) {}
    }

    public static Record generateAnSingleRecord(Map<String, Object> overrideFields){
        HashMap<String, Object> map = new HashMap<>();
        map.put("orgId", orgId);
        map.put("modelId", modelId);
        map.put("modelIdPath", "/RootModel/".concat(modelId));
        map.put("assetId", assetId);
        map.put("pointId", pointId);
        map.put("time", System.currentTimeMillis());
        map.put("value", Math.random());
        Map<String, Object> attr = new LinkedHashMap<>();
        map.put("attr",attr);
        map	.put("dq", 0L);
        if(overrideFields!=null && !overrideFields.isEmpty()){
            map.putAll(overrideFields);
        }
        Field dataField = null;
        try {
            dataField = objectToField(map);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        Record record = RecordCreator.create();
        record.set(dataField);
        return record;
    }


    public static Record createRecord(){
        return RecordCreator.create("x","DataGenerator");
    }


    public static Field objectToField(Object json) throws IOException {
        Field field;
        if (json == null) {
            field = Field.create(Field.Type.STRING, null);
        } else if (json instanceof List) {
            List jsonList = (List) json;
            List<Field> list = new ArrayList<>(jsonList.size());
            for (Object element : jsonList) {
                list.add(objectToField(element));
            }
            field = Field.create(list);
        } else if (json instanceof Map) {
            Map<String, Object> jsonMap = (Map<String, Object>) json;
            Map<String, Field> map = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                map.put(entry.getKey(), objectToField(entry.getValue()));
            }
            field = Field.create(map);
        } else if (json instanceof String) {
            field = Field.create((String) json);
        } else if (json instanceof Boolean) {
            field = Field.create((Boolean) json);
        } else if (json instanceof Character) {
            field = Field.create((Character) json);
        } else if (json instanceof Byte) {
            field = Field.create((Byte) json);
        } else if (json instanceof Short) {
            field = Field.create((Short) json);
        } else if (json instanceof Integer) {
            field = Field.create((Integer) json);
        } else if (json instanceof Long) {
            field = Field.create((Long) json);
        } else if (json instanceof Float) {
            field = Field.create((Float) json);
        } else if (json instanceof Double) {
            field = Field.create((Double) json);
        } else if (json instanceof byte[]) {
            field = Field.create((byte[]) json);
        } else if (json instanceof Date) {
            field = Field.createDatetime((Date) json);
        } else if (json instanceof BigDecimal) {
            field = Field.create((BigDecimal) json);
        } else if (json instanceof UUID) {
            field = Field.create(json.toString());
        } else {
            throw new IOException(Utils.format("Not recognized type '{}', value '{}'", json.getClass(), json));
        }
        return field;
    }


}
