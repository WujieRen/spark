package com.rwj.offlineAnalysisPrj.mockdata;

import com.rwj.offlineAnalysisPrj.util.DateUtils;
import com.rwj.offlineAnalysisPrj.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by renwujie on 2018/01/09 at 20:06
 *
 * 模拟数据
 *
 */
public class MockData {

    /**
     * 模拟数据
     */
    public static void mock(JavaSparkContext sc, SparkSession ss){
        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[]{"问道听香火锅", "顾城粉巷子", "蛋糕", "黄焖鸡", "重庆小面", "担担面", "九九鸭脖", "国贸大厦", "潮汕牛肉海鲜", "温泉", "烧烤", "串串", "西安牛奶", "裤带面", "哨子面"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for(int i = 0; i < 100; i++) {
            long userId = random.nextInt(50);

            for(int j = 0; j < 10; j++) {
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                Long clickCategoryId = -1L;

                for(int k = 0; k < random.nextInt(100); k++) {
                    long pageId = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = -1L;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(15)];
                    } else if("click".equals(action)) {
                        if(clickCategoryId == -1L) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                    } else if("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userId, sessionId,
                            pageId, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds,
                            Long.valueOf(String.valueOf(random.nextInt(10))));
                    rows.add(row);
                }
            }
        }

        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.LongType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true),
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
                DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.LongType, true)
        ));

        Dataset<Row> df = ss.createDataFrame(rowsRDD, schema);

        df.registerTempTable("user_visit_action");

        //df.printSchema();
        df.write().mode(SaveMode.Append).parquet("T:\\testdata\\sparkprj\\user_visit_action");

        /**
         * ==================================================================
         */

        rows.clear();

        String[] sexes = new String[]{"男", "女"};
        for(int i = 0; i < 100; i++) {
            long userId = i;
            String loginName = "user_" + i;
            String realnName = "name_" + i;
            int age = random.nextInt(60);
            String profession = "profession_" + random.nextInt(100);
            String city = "city_" + random.nextInt(200);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userId, loginName, realnName, age, profession, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("userId", DataTypes.LongType, true),
                DataTypes.createStructField("loginName", DataTypes.StringType, true),
                DataTypes.createStructField("realName", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("profession", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)
        ));

        Dataset<Row> df2 = ss.createDataFrame(rowsRDD, schema2);

        df2.registerTempTable("user_info");

        //df2.printSchema();
        df2.write().mode(SaveMode.Append).parquet("T:\\testdata\\sparkprj\\user_info");

        /**
         * ==================================================================
         */

        rows.clear();

        int[] productStatus = new int[]{0, 1};

        for(int i= 0; i < 100; i++) {
            long productId = i;
            String productInfo = "product" + i;
            String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";

            Row row = RowFactory.create(productId, productInfo, extendInfo);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType,true),
                DataTypes.createStructField("product_name", DataTypes.StringType,true),
                DataTypes.createStructField("extend_info", DataTypes.StringType,true)
        ));

        Dataset df3 = ss.createDataFrame(rowsRDD, schema3);
        //df3.printSchema();

        df3.registerTempTable("product_info");
        df3.write().mode(SaveMode.Append).parquet("T:\\testdata\\sparkprj\\product_info");
    }
}
