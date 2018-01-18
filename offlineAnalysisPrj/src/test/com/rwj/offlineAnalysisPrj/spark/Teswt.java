package com.rwj.offlineAnalysisPrj.spark;

import com.alibaba.fastjson.JSONObject;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.ISessionAggrStatDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.SessionAggrStat;
import com.rwj.offlineAnalysisPrj.util.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.Date;
import java.util.Iterator;

/**
 * Created by renwujie on 2018/01/12 at 13:48
 * <p>
 * 用户访问session分析
 * 1. 按session粒度聚合数据
 * -》 生成模拟数据
 * -》 获取响应task参数并从表中取得参数范围内数据
 * -》 对行为数据按session粒度进行聚合
 * -》 首先可将行为数据按session_id进行grouoby，此时得到的数据粒度就是session粒度了。
 * -》 将session粒度的数据和用户信息进行join，就可以获得包含用户信息的session粒度的数据了。
 * 2. 按筛选参数对session粒度聚合数据进行过滤
 * 3. session聚合统计
 * ① 自定义Accumulator
 * ② 重构实现思路 && 重构session聚合
 * ③ 重构过滤进行统计
 * ④ 计算统计结果 -》 写入mysql
 * ⑤ 聚合统计本地测试
 * ⑥ 使用Scala实现自定义Accumulator
 * 4.随机抽取功能
 * ① 计算每天每小时session数量
 * ② 按时间比例抽取算法实现
 * ③ 根据索引进行抽取
 * ④ 抽取session明细数据
 * 5.top10热门品类
 * ① 获取session访问过的所有品类
 * ② 计算各品类点击、下单和支付的次数
 * ③ join品类 与 点击下单支付次数
 * ④ 自定义二次排序key
 * ⑤ 进行二次排序
 * ⑥ 获取top10品类并写入mysql
 * ⑦ Scala实现二次排序
 * 6.top10活跃session
 * ① 开发准备 && top10品类RDD生成
 * ② top10品类被各session点击次数
 * ③ 分组取topN算法获取top10session
 */
public class Teswt {

    public static void main(String[] args) {

        /*args = new String[]{"1"};

        SparkSession ss = SparkSession.builder()
                .master("local[*]")
                .appName("UserVisitSessionAnalyzeSpark")
                //.enableHiveSupport()
                .getOrCreate();
        SparkContext sc = ss.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        //生成模拟数据
        MockData.mock(jsc, ss);

        //查询指定任务并获取响应参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //从user_visit_action表中查询出指定日期的数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(ss, taskParam);
        System.out.println(actionRDD.first().toString());

        //对数据按照sessionId进行groupBy(聚合)，然后与用户信息进行join就是session粒度的包含session和用户信息的数据了。
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(ss, actionRDD);
        System.out.println(sessionid2AggrInfoRDD.count() + "----------------------------" + sessionid2AggrInfoRDD.first().toString());

        //重构，同时进行统计和过滤
        //注册自定义过滤器。reference:http://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators
        AccumulatorV2<String, String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        jsc.sc().register(sessionAggrStatAccumulator);

        //按筛选参数对session粒度聚合数据进行过滤
        //相当于自己编写的算子，是要访问外部任务参数对象的。
        //匿名内部类(算子函数)，访问外部对象，要将外部对象用final修饰
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        System.out.println(filteredSessionid2AggrInfoRDD.count() + "----------------------------" + filteredSessionid2AggrInfoRDD.first().toString());

        //计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        ss.close();*/
    }

    /**
     * 按给定参数过滤session数据
     * 重构：
     *  过滤的同时对各访问时长和步长进行统计
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String,String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final AccumulatorV2<String, String>  sessionAggrStatAccumulator) {
        String startAge = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_END_AGE);
        String professionals = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
                + (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }
        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        //获取聚合数据
                        String aggrInfo = tuple._2;

                        //依次按照筛选参数过滤
                        //年龄
                        if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        //职业
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        //城市
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }
                        //性别
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                            return false;
                        }
                        //搜索词
                        if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }
                        //点击品类id
                        if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        /**
                         * 能走到这里的session都是过滤后剩下的，此时，要对通过滤的session个数、以及各个session的访问时长和步长，进行计数
                         */

                        //session个数
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                        //访问时长和访问步长
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
                        //对该访问时长和步长所在范围计数
                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 访问时长计数
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if(visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if(visitLength >=4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if(visitLength >=7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if(visitLength >=10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if(visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if(visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if(visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if(visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if(visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 访问步长计数
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if(stepLength >=1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if(stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if(stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if(stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if(stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if(stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                }
        );
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 按session粒度聚合数据
     * @param ss
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SparkSession ss, JavaRDD<Row> actionRDD) {
        JavaPairRDD<String, Row> sessionId2ActionRDD = actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }
                });

        //对数据按session粒度进行聚合
        JavaPairRDD<String, Iterable<Row>> sessionIdGroupRDD = sessionId2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)...>
        //提取需要的信息
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionIdGroupRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

                        Long userId = null;

                        // session的起始和结束时间
                        Date startTime = null;
                        Date endTime = null;
                        // session的访问步长，现在的数据格式<sessionId, <info1,info2...>>，每个sessionId后面有几个info，步长就是几
                        int stepLength = 0;

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (userId == null) {
                                userId = row.getLong(1);
                            }

                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }

                            if (clickCategoryId != null && clickCategoryId != -1L) {
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            /**
                             * 重构之访问时长和步长统计
                             */
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if(startTime == null) {
                                startTime = actionTime;
                            }
                            if(endTime == null) {
                                endTime = actionTime;
                            }
                            if(actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if(actionTime.after(endTime)) {
                                endTime = actionTime;
                            }
                            //步长统计
                            stepLength++;
                        }

                        //点击关键词和搜索关键词
                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //访问时长
                        long visitLength =  (endTime.getTime() - startTime.getTime()) / 1000;

                        //因为要和用户数据聚合，所以这里返回<userId, info>的形式
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds+ "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength+ "|"
                                + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);

                        return new Tuple2<Long, String>(userId, partAggrInfo);
                    }
                });

        //查询所有用户数据，并映射成<userId, Row>形式
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRDD = ss.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //将 session粒度的聚合数据 与 用户数据 进行聚合
        JavaPairRDD<Long, Tuple2<String, Row>> userId2FullInfoRDD = userid2PartAggrInfoRDD.join(userId2InfoRDD);

        JavaPairRDD<String, String> sessionId2FullInfoRDD = userId2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|"
                                + Constants.FIELD_AGE + "=" + age + "|"
                                + Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
                                + Constants.FIELD_CITY + "=" + city + "|"
                                + Constants.FIELD_SEX + "=" + sex;

                        return new Tuple2<String, String>(sessionId, fullAggrInfo);
                    }
                });

        //函数最终返回结果
        return sessionId2FullInfoRDD;
    }

    /**
     * 获取指定日期内的数据
     *
     * @param ss
     * @param taskParam
     * @return
     */
    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession ss, JSONObject taskParam) {
        String startDate = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_END_DATE);

        String sql = "select * "
                + "from user_visit_action "
                + "where date>='" + startDate + "' "
                + "and date<='" + endDate + "'";

        Dataset<Row> actionDF = ss.sql(sql);
        return actionDF.javaRDD();
    }

    /**
     * 计算各session范围占比，并写入mysql
     * @param value
     * @param taskId
     */
    private static void calculateAndPersistAggrStat(String value, long taskId) {
        //从Accumulator统计结果中获取响应字段的值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", Constants.STEP_PERIOD_60));

        //计算各个访问时长和访问步长的范围
        //TODO:这块儿其实有一个问题我不太明白，如果这样的结果累加不等于1咋办，这个误差能接受么???
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double)step_length_60 / (double)session_count, 2);

        //将统计结果封装为domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTask_id(taskId);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对应DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }

}