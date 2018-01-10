package com.rwj.offlineAnalysisPrj.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Created by renwujie on 2018/01/05 at 15:03
 */
public class ParamUtils {
    /**
     * 从命令行参数中提取任务id
     * @param args 命令行参数
     * @return 任务id
     */
    public static Long getTaskIdFromArgs(String[] args){
        try {
            if(args != null && args.length > 0){
                return Long.valueOf(args[0]);
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 从JSON对象中提取参数
     * @param jsonObject JSON对象
     * @return 参数
     */
    public static String getParamFromJsonObject(JSONObject jsonObject, String field) {
        JSONArray  jsonArray = jsonObject.getJSONArray(field);
        if(jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        
        return null;
    }



}
