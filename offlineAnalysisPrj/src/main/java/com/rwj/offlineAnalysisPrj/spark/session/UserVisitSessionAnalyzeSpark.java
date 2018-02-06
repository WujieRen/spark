package com.rwj.offlineAnalysisPrj.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.*;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.*;
import com.rwj.offlineAnalysisPrj.mockdata.MockData;
import com.rwj.offlineAnalysisPrj.spark.session.accumulator.SessionAggrStatAccumulator;
import com.rwj.offlineAnalysisPrj.util.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

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
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        SparkSession ss = SparkUtils.getSparkSesseion(Constants.SPARK_APP_NAME_SESSION);

        SparkContext sc = ss.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

        //生成模拟数据
        MockData.mock(jsc, ss);

        //查询指定任务并获取响应参数
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDAO.findById(taskId);
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //从user_visit_action表中查询出指定日期的数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(ss, taskParam);

        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
        //后面又用到了
        //sessionid2actionRDD.cache();
        /**
         * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，第二选择
         * StorageLevel.MEMORY_AND_DISK()，第三选择
         * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
         * StorageLevel.DISK_ONLY()，第五选择
         *
         * 如果内存充足，要使用双副本高可靠机制
         * 选择后缀带_2的策略
         * StorageLevel.MEMORY_ONLY_2()
         *
         */
        sessionid2actionRDD = sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());


        //对数据按照sessionId进行groupBy(聚合)，然后与用户信息进行join就是session粒度的包含session和用户信息的数据了。
        //Tuple2<String, String>(sessionId, fullAggrInfo)
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(ss, sessionid2actionRDD);
        //sessionid2actionRDD.cache();
        //System.out.println(sessionid2AggrInfoRDD.cache().count() + "---" + sessionid2AggrInfoRDD.first().toString());

        //重构，同时进行统计和过滤
        //注册自定义过滤器。reference:http://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators
        AccumulatorV2<String, String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        jsc.sc().register(sessionAggrStatAccumulator, "sessionFilterAndCountAccumulator");

        //按筛选参数对session粒度聚合数据进行过滤
        //相当于自己编写的算子，是要访问外部任务参数对象的。
        //匿名内部类(算子函数)，访问外部对象，要将外部对象用final修饰
        //Tuple2<String, String>(sessionId, fullAggrInfo)
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
        //System.out.println(filteredSessionid2AggrInfoRDD.cache().count() + "---" + filteredSessionid2AggrInfoRDD.first().toString());

        //生成公共的RDD：通过筛选条件的session的访问明细数据
        JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD( sessionid2actionRDD, filteredSessionid2AggrInfoRDD);

        randomExtractSession(task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);//这里原来写错了，应该是用的sessionid2detailRDD，原来写成了sessionid2actionRDD

        //计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);//这儿也搞错了，原来也写成了sessioinid2actionRDD

        //生成公共的RDD：通过筛选条件的session的访问明细数据
        //TODO:因为没有涉及到action，所以要把这个放在action动作之前。否则不会生成需要的结果
        //JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD( sessionid2actionRDD, filteredSessionid2AggrInfoRDD);

        getTop10Session(jsc, taskId, top10CategoryList, sessionid2detailRDD);

        ss.close();
    }

    /**
     * 获取top10活跃session
     * @param taskId
     * @param sessionid2detailRDD
     */
    private static void getTop10Session(JavaSparkContext jsc,
                                        final long taskId,
                                        List<Tuple2<CategorySortKey, String>> top10CategoryList,
                                        JavaPairRDD<String, Row> sessionid2detailRDD) {
        /**
         * 第一步：将top10热门品类的id，生成一份RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
        for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRDD = jsc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算 category 被 各session 点击的次数 <categoryId, <sessionId, count>>
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();
        JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        Map<Long, Long> categoryCountMap = new HashMap<>();
                        // 计算出该session，对每个品类的点击次数
                        while(iterator.hasNext()) {
                            Row row = iterator.next();

                            if(row.getLong(6) != -1) {
                                Long categoryId = row.getLong(6);

                                Long count = categoryCountMap.get(categoryId);
                                if(count == null) {
                                    count = 0L;
                                }

                                count++;

                                categoryCountMap.put(categoryId, count);
                            }
                        }

                        // 返回结果，<categoryid,sessionid,count>格式
                        List<Tuple2<Long, String>> list = new ArrayList<>();
                        for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                            Long categoryId = categoryCountEntry.getKey();
                            Long count = categoryCountEntry.getValue();
                            String value = sessionId + "," + count;
                            list.add(new Tuple2<Long, String>(categoryId, value));
                        }

                        return list.iterator();
                    }
                }
        );

        //关联topN品类和点击次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2sessionCountRDD).mapToPair(
                new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                        return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                    }
                }
        );

        /**
         * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey();
        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                        Long categoryId = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();

                        String[] top10Sessions = new String[10];

                        while(iterator.hasNext()) {
                            String countInfo = iterator.next();
                            Long count = Long.valueOf(countInfo.split(",")[1]);

                            for(int i = 0; i < top10Sessions.length; i++) {
                                if(top10Sessions[i] == null) {
                                    top10Sessions[i] = countInfo;
                                    break;
                                } else {
                                    Long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    //如果新的countInfo的点击次数比原来的大，那么从这一位往后的所有元素都后移一位
                                    if(count > _count) {
                                        for(int j = 9; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j - 1];
                                        }
                                        top10Sessions[i] = countInfo;
                                        break;
                                    }
                                }
                            }
                        }

                        //将数据写入MySQL
                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        for(String sessionCount : top10Sessions) {
                            String sessionId = sessionCount.split(",")[0];
                            Long count = Long.valueOf(sessionCount.split(",")[1]);

                            Top10Session top10Session = new Top10Session();
                            top10Session.setTaskId(taskId);
                            top10Session.setCategoryId(categoryId);
                            top10Session.setSessionId(sessionId);
                            top10Session.setClickCount(count);
                            top10SessionDAO.insert(top10Session);

                            list.add(new Tuple2<>(sessionId, sessionId));
                        }

                        return list.iterator();
                    }
                }
        );

        top10SessionRDD.count();

        /**
         * 第四步：获取top10活跃session的明细数据，并写入MySQL
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD);
        sessionDetailRDD.foreach(
                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
                    @Override
                    public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        Row row = tuple._2._2;

                        SessionDetail sessionDetail = new SessionDetail();
                        sessionDetail.setTaskId(taskId);
                        sessionDetail.setUserId(row.getLong(1));
                        sessionDetail.setSessionId(row.getString(2));
                        sessionDetail.setPageId(row.getLong(3));
                        sessionDetail.setActionTime(row.getString(4));
                        sessionDetail.setSearchKeyword(row.getString(5));
                        sessionDetail.setClickCategoryId(row.getLong(6));
                        sessionDetail.setClickProductId(row.getLong(7));
                        sessionDetail.setOrderCategoryIds(row.getString(8));
                        sessionDetail.setOrderProductIds(row.getString(9));
                        sessionDetail.setPayCategoryIds(row.getString(10));
                        sessionDetail.setPayProductIds(row.getString(11));

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insert(sessionDetail);
                        //System.out.println("插入成功///");
                    }
                }
        );
    }

    /**
     * 获取通过筛选条件的session的访问明细数据RDD
     * @param sessionid2actionRDD
     * @param filteredSessionid2AggrInfoRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2detailRDD(JavaPairRDD<String, Row> sessionid2actionRDD, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD) {
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).mapToPair(
                new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Row row = tuple._2._2;
                        return new Tuple2<>(sessionId, row);
                    }
                }
        );
        return sessionid2detailRDD;
    }

    /**
     * 连接品类RDD与数据RDD
     * @param categoryidRDD
     * @param clickCategoryId2CountRDD
     * @param orderCategoryId2CountRDD
     * @param payCategoryId2CountRDD
     * @return
     */
    private static JavaPairRDD<Long,String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD, JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD, JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
        //如果用leftOuterJoin，就可能出现，右边那个RDD中join过来时没有值的情况。
        //所以Tuple的第二个元素用OPtional<Long>类型，代表可能有值，可能没有值
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long clickCount = 0L;

                        if(optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryId + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                        return new Tuple2<Long, String>(categoryId, value);
                    }
                }
        );

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long orderCount = 0L;

                        if(optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        String value = tuple._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                }
        );

        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        Long categoryId = tuple._1;
                        Optional<Long> optional = tuple._2._2;
                        long payCount = 0L;

                        if(optional.isPresent()) {
                            payCount = optional.get();
                        }

                        String value = tuple._2._1 + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryId, value);
                    }
                }
        );

        return tmpMapRDD;
    }

    /**
     * 获取top10热门品类
     *  参数中的条件满足的session才算，如某个品类，搜索的某个词等
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2actionRDD
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskId, JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {
        /**
         * 第一步：取得符合条件的session访问过的所有品类
         */
        JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        String sessionId = tuple._1;
                        Row row = tuple._2._2;
                        return new Tuple2<String, Row>(sessionId, row);
                    }
                }
        );

        // 访问过：指的是，点击过 或 下单过 或 支付过的品类
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        //点击
                        Long clickCategoryId = row.getLong(6);
                        if(clickCategoryId != -1L) {
                            list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
                        }
                        //下单
                        String orderCategoryIds = row.getString(8);
                        if(orderCategoryIds != null) {
                            String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                            for(String orderCategoryId : orderCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                            }
                        }
                        //支付
                        String payCategoryIds =  row.getString(10);
                        if(payCategoryIds != null) {
                            String[] payCategoryIdsSplited = payCategoryIds.split(",");
                            for(String payCategoryId : payCategoryIdsSplited) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                            }

                        }
                        //最终list存放的其实是点击过 或 下单过 或 支付过 的产品的categoryId。且很可能有重复的。
                        return list.iterator();
                    }
                }
        );

        /**
         * 必须要进行去重
         * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        //TODO:能不能用Set啥的？为啥非要用list???
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */

        // 访问明细中，其中三种访问行为是：点击、下单和支付
        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD =
                getClickCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD =
                getOrderCategoryId2CountRDD(sessionid2detailRDD);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRDD =
                getPayCategoryId2CountRDD(sessionid2detailRDD);

        /**
         * 第三步：join各品类与它的点击、下单和支付的次数
         *
         * categoryIdRDD中，各品类点击、下单和支付的次数，可能不是包含所有品类的。
         *  如：有的品类，就只是被点击过，但是没有人下单和支付。
         *
         * 所以这里，就不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryRDD不能join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryRDD还是要保留下来，只不过没有join到那个数据就是0了。
         */
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
                payCategoryId2CountRDD);

        /**
         * 第四步：自定义二次排序key
         */

        /**
         * 第五步：将数据映射成自定义的排序key(CategorySortKey)，并进行排序
         */
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
                new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {
                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;

                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);

                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }
                }
        );

        //排序——降序
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        /**
         * 第六步：用take(10)取出top10热门品类，并写入MySQL
         */
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        //List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.collect();

        for(Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            String aggrInfo = tuple._2;
            long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category top10Category = new Top10Category();
            top10Category.setTaskId(taskId);
            top10Category.setCategoryId(categoryId);
            top10Category.setClickCount(clickCount);
            top10Category.setOrderCount(orderCount);
            top10Category.setPayCount(payCount);

            top10CategoryDAO.insert(top10Category);
        }
        return top10CategoryList;
    }

    /**
     * 获取各个品类的支付次数RDD
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        //①： 过滤获取有点击行为的RDD
        JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> v) throws Exception {
                        Row row = v._2;
                        return row.getString(10) != null ? true : false;
                    }
                }
        );
        //②： 过滤后对出现的clickCategoryId组合成<clickCategoryId, 1L>的形式
        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        String payCategoryIds = tuple._2.getString(10);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        for(String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list.iterator();
                    }
                }
        );
        //③： 对上一步的结果reduceByKey()即可得到<clickCategory, count>
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return payCategoryId2CountRDD;
    }

    /**
     * 获取各品类的下单次数RDD
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        //①： 过滤获取有点击行为的RDD
        JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v) throws Exception {
                Row row = v._2;
                return row.getString(8) != null ? true : false;
            }
        });
        //②： 过滤后对出现的clickCategoryId组合成<clickCategoryId, 1L>的形式
        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        String orderCategoryIds = tuple._2.getString(8);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<>();

                        for(String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list.iterator();
                    }
                }
        );
        //③： 对上一步的结果reduceByKey()即可得到<clickCategory, count>
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各品类点击次数RDD
     * @param sessionid2detailRDD
     * @return
     */
    private static JavaPairRDD<Long,Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
        //①： 过滤获取有点击行为的RDD
        JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(new Function<Tuple2<String, Row>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Row> v) throws Exception {
                Row row = v._2;

                return (row.getLong(6) != -1L) ? true : false;
            }
        });

        //②： 过滤后对出现的clickCategoryId组合成<clickCategoryId, 1L>的形式
        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String,Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                        Long clickCategoryId = tuple._2.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                }
        );

        //③： 对上一步的结果reduceByKey()即可得到<clickCategory, count>
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return clickCategoryId2CountRDD;
    }

    /**
     * 随机抽取session
     *
     * @param taskId
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(final long taskId, JavaPairRDD<String, String> sessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {
        //1.计算出每天每小时session数量，
        // 先获取JavaPairRDD<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        //然后countByKey()
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;
                        String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(startTime);
                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }
                }
        );

        //Map<String, Long>(dateHour, count)
        Map<String, Long> countMap = time2sessionidRDD.countByKey();

        //2.使用按时间比例抽取算法，取出 每天 每小时 抽取session索引
        //<String, Tuple2<String, List<Integer>(...)>>

        //①将<dateHour, count> -> <date, <hour, count>>
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            String[] dateHour = entry.getKey().split("_");
            String date = dateHour[0];
            String hour = dateHour[1];
            long count = Long.valueOf(entry.getValue());

            //这里当get 新date 时，值一定为null，这时要新建一个容器，来存放这个新date的Map。而如果这个 date 是已存在的，就不需要新建，直接添加在旧容器中即可。
           /* Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                hourCountMap.put(hour, count);
            } else {
                hourCountMap.put(hour, count);
            }
            dateHourCountMap.put(date, hourCountMap);*/
            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if(hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }

            hourCountMap.put(hour, count);
        }

        //②算出每天要抽取的数量，假设总共抽取100个。
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        /**
         * 按时间随机抽取算法实现
         */
        // <date,<hour,(3,5,20,102)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap =
                new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();

        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            //获取 session总数/天
            long sessionCount = 0L;
            for (Long tmpCount : hourCountMap.values()) {
                sessionCount += tmpCount;
            }

            //
            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            //遍历每小时，取 (session/每小时 / sesseion总数/天) * 取session总数 = 每天每小时要取数目
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                //每天每小时要获取多少
                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                //当get 新hour 时，值一定时null，需要新建容器。
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //生成随机索引
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }
            }
        }

        //③遍历每天每小时session，然后根据随机索引进行抽取
        JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

        //TODO:Debug时这里总是不执行，因为没有遇到action动作，相当于这里没有触发，所以
        JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                        String[] dateHour = tuple._1.split("_");
                        String date = dateHour[0];
                        String hour = dateHour[1];
                        Iterator<String> iterator = tuple._2.iterator();
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

                        List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getsessionRandomExtractDAO();

                        int index = 0;
                        while(iterator.hasNext()) {
                            //TODO：这里不明白，为什么这个换个位置差别会这么大???---因为换位置到i的值大于RDD的总数时成死循环了。
                            String sessionAggrInfo = iterator.next();

                            if(extractIndexList.contains(index)) {
                                String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
                                sessionRandomExtract.setTaskId(taskId);
                                sessionRandomExtract.setSessionId(sessionId);

                                sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
                                        sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                extractSessionids.add(new Tuple2<String, String>(sessionId, sessionId));
                            }
                            index++;
                        }
                        return  extractSessionids.iterator();
                    }
                }
        );

        //④：获取抽取出来的session的明细数据
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD);

        extractSessionDetailRDD.foreach(
                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
                    @Override
                    public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        Row row = tuple._2._2;



                        SessionDetail sessionDetail = new SessionDetail();
                        sessionDetail.setTaskId(taskId);
                        sessionDetail.setUserId(row.getLong(1));//userId
                        sessionDetail.setSessionId(row.getString(2));//sessionId
                        sessionDetail.setPageId(row.getLong(3));//pageId
                        sessionDetail.setActionTime(row.getString(4));//actionTime
                        sessionDetail.setSearchKeyword(row.getString(5));//searchKeyword
                        sessionDetail.setClickCategoryId(row.getLong(6));//clickCategoryId
                        sessionDetail.setClickProductId(row.getLong(7));//clickProductId
                        sessionDetail.setOrderCategoryIds(row.getString(8));//orderCategoryIds
                        sessionDetail.setOrderProductIds(row.getString(9));//orderProductIds
                        sessionDetail.setPayCategoryIds(row.getString(10));//payCategoryIds
                        sessionDetail.setPayProductIds(row.getString(11));//payProductIds

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insert(sessionDetail);
                        //System.out.println("插入成功g---");
                    }
                }
        );
    }

    /**
     * 按给定参数过滤session数据
     * 重构：
     * 过滤的同时对各访问时长和步长进行统计
     *
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final AccumulatorV2<String, String> sessionAggrStatAccumulator) {
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
                + (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
        if (_parameter.endsWith("\\|")) {
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
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }
                        //职业
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }
                        //城市
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }
                        //性别
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                            return false;
                        }
                        //搜索词
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }
                        //点击品类id
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
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
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 访问步长计数
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                }
        );
        return filteredSessionid2AggrInfoRDD;
    }

    /**
     * 按session粒度聚合数据
     *
     * @param ss
     * @param sessionid2actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(SparkSession ss, JavaPairRDD<String, Row> sessionid2actionRDD) {
        //对数据按session粒度进行聚合
        JavaPairRDD<String, Iterable<Row>> sessionIdGroupRDD = sessionid2actionRDD.groupByKey();

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

                            if (clickCategoryId != -1L) {
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            /**
                             * 重构之访问时长和步长统计
                             */
                            Date actionTime = DateUtils.parseTime(row.getString(4));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }
                            //步长统计
                            stepLength++;
                        }

                        //点击关键词和搜索关键词
                        String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        //访问时长
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        //因为要和用户数据聚合，所以这里返回<userId, info>的形式
                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
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
        //JavaPairRDD<Long, String> userid2PartAggrInfoRDD
        //JavaPairRDD<Long, Row> userId2InfoRDD
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
     * 计算各session范围占比，并写入mysql
     *
     * @param value
     * @param taskId
     */
    private static void calculateAndPersistAggrStat(String value, long taskId) {
        //从Accumulator统计结果中获取响应字段的值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        //System.out.println(session_count);

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
        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

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

    /**
     * 获取sessionid2到访问行为数据的映射的RDD
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }

        });
    }

}
