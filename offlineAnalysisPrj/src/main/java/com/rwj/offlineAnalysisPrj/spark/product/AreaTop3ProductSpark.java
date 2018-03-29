package com.rwj.offlineAnalysisPrj.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.rwj.offlineAnalysisPrj.conf.ConfiguratoinManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.IAreaTop3ProductDAO;
import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.AreaTop3Product;
import com.rwj.offlineAnalysisPrj.domain.Task;
import com.rwj.offlineAnalysisPrj.util.ParamUtils;
import com.rwj.offlineAnalysisPrj.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 各区域top3热门商品统计spark作业
 * Created by renwujie on 2018/03/20 at 17:44
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args){
        //1、构建Spark上下文
        SparkSession ss = SparkUtils.getSparkSesseion(Constants.SPARK_APP_NAME_PRODUCT);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        ss.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        ss.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
        ss.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
        ss.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        ss.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);

        //2、生成模拟数据
        SparkUtils.mockData(jsc, ss);

        //3、查询任务，获取任务参数
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
            Task task = taskDAO.findById(taskId);
            if(task == null) {
                System.out.println(new Date() + ": cannot find this task with id [" + taskId + "].");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
        String startDate = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParamFromJsonObject(taskParam, Constants.PARAM_END_DATE);

        //4、查询指定日期范围内的点击行为数据
        JavaPairRDD<Long, Row> cityid2clickActionRDD = getClickActionRDDByDate(ss, startDate, endDate);

        //5、从Mysql中查询城市信息
        JavaPairRDD<Long,Row> cityid2CityInfoRDD = getcityid2CityInfoRDD(ss);

        generateTempClickProductBasicTable(ss, cityid2clickActionRDD, cityid2CityInfoRDD);

        generateTempAreaPrdocutClickCountTable(ss);

        generateTempAreaFullProductClickCountTable(ss);

        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(ss);

        List<Row> rows = areaTop3ProductRDD.collect();
        persistAreaTop3Product(taskId, rows);

        ss.close();
    }

    /**
     * 将计算出来的各区域top3热门商品写入MySQL中
     * @param rows
     */
    private static void persistAreaTop3Product(long taskid, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<AreaTop3Product>();

        for(Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskid);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(row.getLong(3));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areaTop3ProductDAO.insertBatch(areaTop3Products);
    }

    /**
     * 获取各区域top3热门商品
     * @param ss SparkSession
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession ss) {
        //技术点在开窗函数
        //使用开窗函数新进行一个子查询：按照area进行分组，对每个分组数据再按照click_count进行排序，打一个组内的行号
        //接着在外层查询中，过滤出各个组内行号前3的数据，就是各个区域下的top3热门品类

        String sql =
                "SELECT " +
                        "area, " +
                        "CASE " +
                            "WHEN area='华北' OR area='华东' THEN 'A级' " +
                            "WHEN area='华南' OR area='华中' THEN 'B级' " +
                            "WHEN area='西北' OR area='西南' THEN 'C级' " +
                            "ELSE 'D级' " +
                        "END area_level, " +
                        "product_id, " +
                        "click_count, " +
                        "city_infos, " +
                        "product_name, " +
                        "product_status " +
                "FROM (" +
                        "SELECT " +
                            "area, " +
                            "product_id, " +
                            "click_count, " +
                            "city_infos, " +
                            "product_name, " +
                            "product_status, " +
                            "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank " +
                        "FROM tmp_area_fullprod_click_count " +
                ") t " +
                "WHERE rank<=3";

        Dataset df = ss.sql(sql);

        return df.javaRDD();
    }

    /**
     *area,product_id,click_count,city_infos,product_names,product_staus
     * @param ss SparkSession
     */
    private static void generateTempAreaFullProductClickCountTable(SparkSession ss) {
        String sql =
                "SELECT " +
                        "tapcc.area, " +
                        "tapcc.product_id, " +
                        "tapcc.click_count, " +
                        "tapcc.city_infos, " +
                        "pi.product_name," +
                        "if(get_json_object(pi.extend_info, 'product_status')=0, '自营商品', '第三方商品') product_status " +
                "FROM tmp_area_product_click_count tapcc " +
                "JOIN product_info pi " +
                "ON tapcc.product_id = pi.product_id ";
        Dataset df = ss.sql(sql);

        df.registerTempTable("tmp_area_fullprod_click_count");
    }

    /**
     * area,productId,click_count,city_indo
     * @param ss SparkSession
     */
    private static void generateTempAreaPrdocutClickCountTable(SparkSession ss) {
        String sql =
                "SELECT " +
                        "area, " +
                        "product_id, " +
                        "count(*) click_count, " +
                        "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
                "FROM tmp_clk_prod_basic " +
                "GROUP BY area,product_id ";

        Dataset df = ss.sql(sql);

        df.registerTempTable("tmp_area_product_click_count");
    }

    /**
     * 生成点击商品基础信息临时表
     * @param ss SparkSession
     * @param cityid2clickActionRDD 点击信息
     * @param cityid2CityInfoRDD 城市信息
     */
    private static void generateTempClickProductBasicTable(
            SparkSession ss,
            JavaPairRDD<Long, Row> cityid2clickActionRDD,
            JavaPairRDD<Long, Row> cityid2CityInfoRDD) {
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRDD.join(cityid2CityInfoRDD);

        JavaRDD<Row> mappedRDD = joinedRDD.map(
                new Function<Tuple2<Long,Tuple2<Row,Row>>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple) throws Exception {
                        long cityId = tuple._1;
                        Row clickAction = tuple._2._1;
                        Row cityInfo = tuple._2._2;

                        long productId = clickAction.getLong(1);
                        String cityName = cityInfo.getString(1);
                        String area = cityInfo.getString(2);

                        return RowFactory.create(cityId, cityName, area, productId);
                    }
                }
        );

        // 基于JavaRDD<Row>的格式，可以将其转换为DataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFields);
        Dataset df = ss.createDataFrame(mappedRDD, schema);

        df.registerTempTable("tmp_clk_prod_basic");
    }

    /**
     * 从MySql中查询城市信息
     * @param ss SparkSession
     * @return
     */
    private static JavaPairRDD<Long, Row> getcityid2CityInfoRDD(SparkSession ss) {
        String url = null;
        String userName = null;
        String password = null;
        boolean local = ConfiguratoinManager.getBooleanValue(Constants.SPARK_LOCAL);

        if(local) {
            url = ConfiguratoinManager.getProperty(Constants.JDBC_URL);
            userName = ConfiguratoinManager.getProperty(Constants.JDBC_USER);
            password = ConfiguratoinManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfiguratoinManager.getProperty(Constants.JDBC_URL_PROD);
            userName = ConfiguratoinManager.getProperty(Constants.JDBC_URL_PROD);
            password = ConfiguratoinManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("user", userName);
        options.put("password", password);
        options.put("dbtable", "city_info");

        //通过SparkSession去Mysql中查询
        Dataset cityInfoDF = ss.read().format("jdbc")
                .options(options).load();

        //返回RDD
        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityid2CityInfoRDD = cityInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        long city_id = row.getLong(0);
                        return new Tuple2<>(city_id, row);
                    }
                }
        );

        return cityid2CityInfoRDD;
    }

    /**
     * 从user_visit_action中查询指定日期范围内的点击数据
     * @param ss SparkSession
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 指定日期范围内的RDD
     */
    private static JavaPairRDD<Long, Row> getClickActionRDDByDate(SparkSession ss, String startDate, String endDate) {
        //从user_visit_action中查找：①click_product_id不为null的数据，即为点击数据。②在指定日期范围内的。
        String sql = "SELECT " +
                    "city_id, " +
                    "click_product_id " +
                "FROM user_visit_action " +
                "WHERE click_product_id != -1 " +
                "AND date >= '" + startDate + "'" +
                "AND date <= '" + endDate + "'";
        Dataset clickActionDf = ss.sql(sql);

        JavaRDD clickActionRDD = clickActionDf.javaRDD();
        JavaPairRDD<Long, Row> cityid2clickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        long city_id = row.getLong(0);
                        return new Tuple2<>(city_id, row);
                    }
                }
        );

        return cityid2clickActionRDD;
    }
}
