package com.rwj.offlineAnalysisPrj.spark;

import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.util.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * Created by renwujie on 2018/01/14 at 18:46
 */
public class SessionAggrStatAccumulator2 implements AccumulatorParam<String> {

    /**
     * addInPlace和addAccumulator
     * 可以理解为是一样的
     *
     * 这两个方法，主要是实现，v1就是我们初始化的那个连接串
     * v2，就是我们在遍历session的时候，判断出某个session对应的区间，然后会用Constants.TIME_PERIOD_1s_3s
     * 所以，我们，要做的事情就是
     * 在v1中，找到v2对应的value，累加1，然后再更新回连接串里面去
     *
     */
    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    /**
     * 用于数据的初始化
     * @param initialValue
     * @return
     */
    @Override
    public String zero(String initialValue) {
        return Constants.SESSION_COUNT + "=0|"
                + Constants.TIME_PERIOD_1s_3s + "=0|"
                + Constants.TIME_PERIOD_4s_6s + "=0|"
                + Constants.TIME_PERIOD_7s_9s + "=0|"
                + Constants.TIME_PERIOD_10s_30s + "=0|"
                + Constants.TIME_PERIOD_30s_60s + "=0|"
                + Constants.TIME_PERIOD_1m_3m + "=0|"
                + Constants.TIME_PERIOD_3m_10m + "=0|"
                + Constants.TIME_PERIOD_10m_30m + "=0|"
                + Constants.TIME_PERIOD_30m + "=0|"
                + Constants.STEP_PERIOD_1_3 + "=0|"
                + Constants.STEP_PERIOD_4_6 + "=0|"
                + Constants.STEP_PERIOD_7_9 + "=0|"
                + Constants.STEP_PERIOD_10_30 + "=0|"
                + Constants.STEP_PERIOD_30_60 + "=0|"
                + Constants.STEP_PERIOD_60 + "=0";
    }

    /**
     * session统计计算逻辑
     * @param v1 连接串
     * @param v2 范围区间
     * @return 更新以后的连接串
     */
    private String add(String v1, String v2) {
        //校验，v1为空的话，直接返回v2
        if(StringUtils.isEmpty(v1)) {
            return v2;
        }

        //更新值
        String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
        if(oldValue != null) {
            Integer newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
        }
        return v1;
    }
}
