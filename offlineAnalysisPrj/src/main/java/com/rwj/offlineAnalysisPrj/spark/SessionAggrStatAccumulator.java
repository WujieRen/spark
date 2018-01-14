package com.rwj.offlineAnalysisPrj.spark;

import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.util.StringUtils;
import org.apache.spark.util.AccumulatorV2;

/**
 * Created by renwujie on  2018/01/14 at 15:37
 *
 * reference:
 *  http://www.cnblogs.com/zhangweilun/p/6684776.html
 */
public class SessionAggrStatAccumulator extends AccumulatorV2<String, String> {

    private static String result = Constants.SESSION_COUNT + "=0|"
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

    /**
     * 当AccumulatorV2中存在类似数据不存在这种问题时，是否结束程序
     *
     * @return
     */
    @Override
    public boolean isZero() {
        String newResult = Constants.SESSION_COUNT + "=0|"
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
        return this.result == newResult;
    }

    /**
     * 拷贝一个新的AccumulatorV2
     *
     * @return
     */
    @Override
    public AccumulatorV2 copy() {
        SessionAggrStatAccumulator myAccumulator = new SessionAggrStatAccumulator();
        myAccumulator.result = this.result;
        return myAccumulator;
    }

    /**
     * 重置AccumulatorV2中的数据
     */
    @Override
    public void reset() {
        result = Constants.SESSION_COUNT + "=0|"
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

    @Override
    public void add(String v) {
        String v1 = this.result;
        String v2 = v;
        if (StringUtils.isEmpty(v2)) {
            return;
        } else {
            String newResult = "";
            String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
            if (!oldValue.isEmpty()) {
                Integer newValue = Integer.valueOf(oldValue) + 1;
                newResult = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
            }
            result = newResult;
        }
    }

    /**
     * 合并数据
     *
     * @param other
     */
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        if (other == null) {
            return;
        } else {
            if (other instanceof SessionAggrStatAccumulator) {
                String newResult = "";
                String[] arrys = new String[]{Constants.SESSION_COUNT,
                        Constants.TIME_PERIOD_1s_3s,
                        Constants.TIME_PERIOD_4s_6s,
                        Constants.TIME_PERIOD_7s_9s,
                        Constants.TIME_PERIOD_10s_30s,
                        Constants.TIME_PERIOD_30s_60s,
                        Constants.TIME_PERIOD_1m_3m,
                        Constants.TIME_PERIOD_3m_10m,
                        Constants.TIME_PERIOD_10m_30m,
                        Constants.TIME_PERIOD_30m,
                        Constants.STEP_PERIOD_1_3,
                        Constants.STEP_PERIOD_4_6,
                        Constants.STEP_PERIOD_7_9,
                        Constants.STEP_PERIOD_10_30,
                        Constants.STEP_PERIOD_30_60,
                        Constants.STEP_PERIOD_60,
                };
                for (String v : arrys) {
                    String oldValue = StringUtils.getFieldFromConcatString(((SessionAggrStatAccumulator) other).result, "\\|", v);
                    if (!oldValue.isEmpty()) {
                        Integer newValue = Integer.valueOf(oldValue) + 1;
                        if (newResult.isEmpty()) {
                            newResult = StringUtils.setFieldInConcatString(result, "\\|", v, String.valueOf(newValue));
                        }
                        newResult = StringUtils.setFieldInConcatString(newResult, "\\|", v, String.valueOf(newValue));
                    }
                }
                result = newResult;
            }
        }
    }

    /**
     * AccumulatorV2对外访问的数据结果
     * @return
     */
    @Override
    public String value() {
        return this.result;
    }
}
