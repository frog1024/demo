package com.zp.hive.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Zhao Peng
 * @version V1.0.0
 * @description 从JSON中获取多个Key对应的各个Value
 */
public class BaseFieldUdf extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length == 2) {
            List<ObjectInspector> types = new ArrayList<>();
            List<String> names = new ArrayList<>();
            names.add("json_values");
            ObjectInspector inspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            types.add(inspector);
            return ObjectInspectorFactory.getStandardStructObjectInspector(names, types);
        } else {
            throw new UDFArgumentTypeException(2, "this function must have 2 parameter");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments.length != 2) {
            throw new UDFArgumentTypeException(2, "this function must have 2 parameter");
        }

        String json = arguments[0].get().toString();
        String keyString = arguments[1].get().toString();

        String[] keys = keyString.split(",");
        String[] jsonContents = json.split("\\|");
        if (jsonContents.length != 2 || StringUtils.isBlank(jsonContents[1])) {
            throw new RuntimeException("the json_log type is failure!");
        }

        StringBuilder sb = new StringBuilder();
        try {
            JSONObject jsonObj = new JSONObject(jsonContents[1]);
            JSONObject cmObj = jsonObj.getJSONObject("cm");
            for (String key : keys) {
                String k = key.trim();
                if (cmObj.has(k)) {
                    sb.append(cmObj.getString(k)).append("\t");
                } else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObj.getString("et")).append("\t");
            sb.append(jsonContents[0]).append("\t");
            return sb.toString();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "the value of keys " + children[1] + " in json string " + children[0];
    }
}
