package com.rwj.offlineAnalysisPrj.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.Task;
import com.rwj.offlineAnalysisPrj.mockdata.MockData;
import com.rwj.offlineAnalysisPrj.util.DateUtils;
import com.rwj.offlineAnalysisPrj.util.ParamUtils;
import com.rwj.offlineAnalysisPrj.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * Created by renwujie on 2018/01/25 at 18:04
 *
 * 页面单跳转化率模块spark作业
 */
public class PageOneStepConvertSpark {
    public static void main(String[] args){
        //1、构建上下文
        SparkSession ss = SparkUtils.getSparkSesseion(Constants.SPARK_APP_NAME_PAGE);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        //2、生成模拟数据
        MockData.mock(jsc, ss);

        //3、查询任务，获取任务的参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);
        if(task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //4、查询制定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(ss, taskParam);

        //页面切片的生成，要基于session粒度
        JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2actionRDD(actionRDD);

        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD = sessionid2actionRDD.groupByKey();
        sessionid2actionsRDD = sessionid2actionsRDD.cache();

        // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配，算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(jsc, sessionid2actionsRDD, taskParam);
        Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();

        //获取页面流中初始页面的pv
        long startPagePv = getStartPagePv(taskParam, sessionid2actionsRDD);

    }

    /**
     * 获取页面流中初始页面的pv
     * @param taskParam
     * @param sessionid2actionsRDD
     * @return
     */
    private static long getStartPagePv(JSONObject taskParam,
                                       JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD) {

        String targetPageFlow = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = sessionid2actionsRDD.flatMap(
                new FlatMapFunction<Tuple2<String,Iterable<Row>>, Long>() {
                    @Override
                    public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        Iterator<Row> iterator = tuple._2.iterator();

                        List<Long> list = new ArrayList<Long>();

                        while(iterator.hasNext()) {
                            long pageId = iterator.next().getLong(3);
                            if(pageId == startPageId) {
                                list.add(pageId);
                            }
                        }

                        return list.iterator();
                    }
                }
        );

        return startPageRDD.count();
    }

    /**
     * 页面切片生成与匹配算法
     * @param jsc
     * @param sessionid2actionsRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext jsc, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRDD, JSONObject taskParam) {

        String targetPageFlow = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Broadcast<String> targetPageFlowBroadcast = jsc.broadcast(targetPageFlow);


        return sessionid2actionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {

                        Iterator<Row> iterator = tuple._2.iterator();

                        String[] targetPages = targetPageFlowBroadcast.value().split(",");

                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

                        //对同一session的行为按时间排序
                        List<Row> rows = new ArrayList<Row>();
                        while(iterator.hasNext()) {
                            rows.add(iterator.next());
                        }
                        Collections.sort(rows, new Comparator<Row>() {
                            @Override
                            public int compare(Row o1, Row o2) {
                                String actionTime1 = o1.getString(4);
                                String actionTime2 = o2.getString(4);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return (int)(date1.getTime() - date2.getTime());
                            }
                        });

                        //依次取出并和参数页面流对比
                        Long lastPageId = null;
                        for(Row row : rows) {
                            long pageId = row.getLong(3);

                            if(lastPageId == null) {
                                lastPageId = pageId;
                                continue;
                            }

                            String pageSplit = lastPageId + "_" + pageId;

                            for(int i = 1; i < targetPages.length; i++) {
                                String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];

                                if(pageSplit.equals(targetPageSplit)) {
                                    list.add(new Tuple2<>(pageSplit, 1));
                                    break;
                                }
                            }

                            lastPageId = pageId;
                        }

                        return list.iterator();
                    }
                }
        );
    }

    /**
     * 获取<sessionid,用户访问行为>格式的数据
     * @param actionRDD 用户访问行为RDD
     * @return <sessionid,用户访问行为>格式的数据
     */
    private static JavaPairRDD<String, Row> getSessionid2actionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        String sessionId = row.getString(2);
                        return new Tuple2<>(sessionId, row);
                    }
                }
        );
    }
}
