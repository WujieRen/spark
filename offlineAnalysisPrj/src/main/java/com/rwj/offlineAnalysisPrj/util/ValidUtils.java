package com.rwj.offlineAnalysisPrj.util;

/**
 * Created by renwujie on 2018/01/05 at 15:42
 * <p>
 * 校验工具类
 */
public class ValidUtils {

    /**
     * 校验数据中的指定字段，是否在指定范围内（数值型）
     *
     * @param data            数据
     * @param dataField       数据字段
     * @param parameter       参数
     * @param startParamField 起始参数字段
     * @param endParamField   结束参数字段
     * @return 校验结果
     */
    public static boolean between(String data, String dataField,
                                  String parameter, String startParamField,
                                  String endParamField) {
        String startParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField);
        String endParamFieldStr = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField);
        if (startParamFieldStr == null || endParamFieldStr == null) {
            return true;
        }

        int startSet = Integer.valueOf(startParamFieldStr);
        int endSet = Integer.valueOf(endParamFieldStr);
        String dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField);

        if (dataFieldStr != null) {
            int dataSet = Integer.valueOf(dataFieldStr);

            if (dataSet >= startSet && dataSet <= endSet) {
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否有值与参数字段的值相同。
     * 只有有任何一个值相同就会返回true，都不相同才返回false。
     *
     * @param data       数据
     * @param dataField  数据字段
     * @param parameter  参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean in(String data, String dataField,
                             String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }

        String[] paramFieldValueSplited = paramFieldValue.split(",");
        String dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField);

        if (dataFieldValue != null) {
            String[] dataFieldValueSplited = dataFieldValue.split(",");

            for (String v1 : paramFieldValueSplited) {
                for (String v2 : dataFieldValueSplited) {
                    if (v1.equals(v2)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * 校验数据中的指定字段，是否在指定范围内(字符型)
     *
     * @param data       数据
     * @param dataField  数据字段
     * @param parameter  参数
     * @param paramField 参数字段
     * @return 校验结果
     */
    public static boolean equal(String data, String dataField,
                                String parameter, String paramField) {
        String paramFieldValue = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField);
        if (paramFieldValue == null) {
            return true;
        }

        String dataFieldValue = StringUtils.getFieldFromConcatString(data, "\\|", dataField);
        if (dataFieldValue != null) {
            if (paramFieldValue.equals(dataFieldValue)) {
                return true;
            }
        }

        return false;
    }

}
