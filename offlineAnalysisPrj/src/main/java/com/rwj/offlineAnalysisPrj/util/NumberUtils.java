package com.rwj.offlineAnalysisPrj.util;

import java.math.BigDecimal;

/**
 * Created by renwujie on 2018/01/05 at 14:54
 */
public class NumberUtils {
    /**
     * 格式化小数
     * @param num 数字
     * @param scale 四舍五入的位数
     * @return 格式化小数
     */
    public static double formatDouble(double num, int scale) {
        BigDecimal bigDecimal = new BigDecimal(num);
        Double value = bigDecimal.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
        return value;
    }
}
