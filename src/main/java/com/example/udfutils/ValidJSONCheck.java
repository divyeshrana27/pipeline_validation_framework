package com.example.udfutils;

import java.io.Serializable;

import org.apache.spark.sql.api.java.UDF1;
import org.json.JSONObject;

public class ValidJSONCheck implements UDF1<String,  Boolean>, Serializable{
    @Override
    public Boolean call(String val) throws Exception {
        try{
            JSONObject obj = new JSONObject(val);
        } catch(Exception e){
            return false;
        }
        return true;
    }
}
