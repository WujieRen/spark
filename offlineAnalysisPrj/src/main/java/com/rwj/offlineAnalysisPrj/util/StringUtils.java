package com.rwj.offlineAnalysisPrj.util;

/**
 * Created by renwujie on 2018/01/05 at 15:08
 */
public class StringUtils {
    /**
     * 判断字符串是否为空
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否不为空
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        int length = str.length();
        if(str.startsWith(",")){
            str = str.substring(1);
        }
        if(str.endsWith(",")){
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     * @param str
     * @return
     */
    public static String fulfill(String str) {
        if(str.length() == 2){
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段值
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段
     * @return 字段值
     */
    public static String getFieldFromConcatString(String str,
                                                  String delimiter, String field) {
        String[] fields = str.split(delimiter);
        for(String concatField : fields){
            String[] kv = concatField.split("=");
            if(kv.length == 2){
                String fieldName = kv[0];
                String fieldValue = kv[1];
                if(fieldName.equals(field)){
                    return fieldValue;
                }
            }
        }

        return null;
    }

    /**
     * 从拼接的字符串中给字段设置值
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     */
    public static String setFieldInConcatString(String str,
                                                String delimiter, String field, String newFieldValue){
        String[] fields = str.split(delimiter);

        for(int i = 0; i < fields.length; i++){
            String key = fields[i].split("=")[0];
            if(key.equals(field)){
                fields[i] = key + newFieldValue;

                //替换后就不再进行循环了
                break;
            }
        }

        StringBuffer buffer = new StringBuffer("");
        for(int i = 0; i < fields.length; i++){
            buffer.append(fields[i]);

            if(i < fields.length - 1){
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

}
