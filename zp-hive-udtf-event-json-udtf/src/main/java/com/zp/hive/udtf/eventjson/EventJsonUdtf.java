package com.zp.hive.udtf.eventjson;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhao Peng
 * @version V1.0.0
 * @description eventjson解析udtf
 */
public class EventJsonUdtf extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> names = new ArrayList<>();
        names.add("event_name");
        names.add("event_json");
        List<ObjectInspector> types = new ArrayList<>();
        types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        types.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
    }

    /**
     * 输入一条记录输出多条记录
     *
     * @param args
     * @throws HiveException
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String s = args[0].toString().trim();
        if (StringUtils.isBlank(s)) {
            return;
        } else {
            try {
                JSONArray jsonArray = new JSONArray(s);
                if (jsonArray.length() == 0) {
                    return;
                }
                for (int i = 0; i < jsonArray.length(); i++) {
                    String[] result = new String[2];
                    try {
                        // 取出每个的事件名称（ad/facoriters）
                        result[0] = jsonArray.getJSONObject(i).getString("en");
                        // 取出每一个事件整体
                        result[1] = jsonArray.getString(i);
                    } catch (JSONException e) {
                        continue;
                    }
                    /* 调用forward方法将结果返回*/
                    forward(result);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 当没有记录处理的时候该方法会被调用，用来清理代码或者产生额外的输出
     *
     * @throws HiveException
     */
    @Override
    public void close() throws HiveException {
        /* nothing */
    }
}
