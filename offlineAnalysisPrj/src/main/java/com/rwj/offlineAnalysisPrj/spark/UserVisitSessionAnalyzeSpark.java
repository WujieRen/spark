package com.rwj.offlineAnalysisPrj.spark;

import com.alibaba.fastjson.JSONObject;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.Task;
import com.rwj.offlineAnalysisPrj.mockdata.MockData;
import com.rwj.offlineAnalysisPrj.util.ParamUtils;
import com.rwj.offlineAnalysisPrj.util.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;

/**
 * Created by renwujie on 2018/01/12 at 13:48
 * <p>
 * 用户访问session分析
 * 1. 按session粒度聚合数据
 * -》 生成模拟数据
 * -》 获取响应task参数并从表中取得参数范围内数据
 * -》 对行为数据按session粒度进行聚合
 * -》 首先可将行为数据按session_id进行grouoby，此时得到的数据粒度就是isession粒度了。
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
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .master("local")
                .appName("UserVisitSessionAnalyzeSpark")
                //.enableHiveSupport()
                .getOrCreate();
        SparkContext sc = ss.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        //生成模拟数据
        MockData.mock(jsc, ss);

        //查询指定任务并获取响应参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        //long taskId = ParamUtils.getTaskIdFromArgs(args);
        long taskId = 1;
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //从user_visit_action表中查询出指定日期的数据
        JavaRDD<Row> actionRDD = getActionRDDByDateRange(ss, taskParam);

        //对数据按照sessionId进行groupBy(聚合)，然后与用户信息进行join就是session粒度的包含session和用户信息的数据了。
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(ss, actionRDD);
        System.out.println(sessionid2AggrInfoRDD.first().toString() + "----------------------------");

    }

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
                        }

                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //因为要和用户数据聚合，所以这里返回<userId, info>的形式
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
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

}
