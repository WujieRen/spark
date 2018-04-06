package com.rwj.offlineAnalysisPrj.spark.ad;

import com.rwj.offlineAnalysisPrj.conf.ConfigurationManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import com.rwj.offlineAnalysisPrj.dao.IAdBlackListDAO;
import com.rwj.offlineAnalysisPrj.dao.IAdProvinceTop3DAO;
import com.rwj.offlineAnalysisPrj.dao.IAdStatDAO;
import com.rwj.offlineAnalysisPrj.dao.IAdUserClickCountDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.AdBlackList;
import com.rwj.offlineAnalysisPrj.domain.AdProvinceTop3;
import com.rwj.offlineAnalysisPrj.domain.AdStat;
import com.rwj.offlineAnalysisPrj.domain.AdUserClickCount;
import com.rwj.offlineAnalysisPrj.util.DateUtils;
import com.rwj.offlineAnalysisPrj.util.SparkUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by renwujie on 2018/03/29 at 14:11
 * <p>
 * 广告点击流量实时统计spark作业
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) {
        //SparkStreaming上下文构建
        SparkSession ss = SparkUtils.getSparkSesseion(Constants.SPARK_APP_NAME_ADVERTISEMENT);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(5));

        //创建针对Kafka数据来源的输入DStream(离线流，代表了一个源源不断的数据来源)
        // 选用kafka direct api（很多好处，包括自己内部自适应调整每次接收数据量的特性，等等）

        // 构建kafka参数map,主要要放置的就是，要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty("kafka.metadata.broker.list"));

        //构建kafka Topic
        String kafkaTopics = ConfigurationManager.getProperty("kafka.topic");
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<>();
        for (String kafkaTopic : kafkaTopicsSplited) {
            topics.add(kafkaTopic);
        }

        //基于kafka direct api模式，构建出针对kafka集群中指定topic的输入DStream
        //TODO:这个返回值为K,V。V是日志，K是什么?(没有实际意义)
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        //先过滤一遍，命中黑名单的去除
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlackList(adRealTimeLogDStream);

        //不在黑名单中的记录fiteredAdRealTimeLogDStream才判断访问次数是否大于100，然后再判断是否加入黑名单
        generateDynamicBlackList(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        // 最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);
        //


        // 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
        try {
            jssc.start();
            jssc.awaitTermination();
            jssc.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    /**
     * 计算最近1小时窗口内的广告点击趋势
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(
            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String,String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        // timestamp province city userid adid
                        String[] logSplited = tuple._2.split(" ");
                        String timeMinute = DateUtils.formatTimeMinute(
                                new Date(Long.valueOf(logSplited[0])));
                        long adId = Long.valueOf(logSplited[4]);

                        return new Tuple2<String, Long>(timeMinute + "_" + adId, 1L);
                    }
                }
        );

        // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
        // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势
        // 点图 / 折线图
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                },
                Durations.minutes(60),
                Durations.seconds(10)
                //每隔10秒钟计算一下每分钟的数据，reference: https://blog.csdn.net/qq_20641565/article/details/76906686
        );

        // aggrRDD每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
        // 各广告，在最近1小时内，各分钟的点击量
        aggrRDD.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {

                                    }
                                }
                        );
                    }
                }
        );




    }


    /**
     * 计算每天各省份的top3热门广告
     *
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(SparkSession ss,
                                                JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // adRealTimeStatDStream
        // 每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                        // <yyyyMMdd_province_city_adid, clickCount>
                        // <yyyyMMdd_province_adid, clickCount>
                        JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, Long>, String, Long>() {
                                    @Override
                                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                                        String[] keySplited = tuple._1.split("_");
                                        String date = keySplited[0];
                                        String province = keySplited[1];
                                        long adid = Long.valueOf(keySplited[3]);
                                        long clickCount = tuple._2;

                                        String key = date + "_" + province + "_" + adid;

                                        return new Tuple2<String, Long>(key, clickCount);
                                    }
                                }
                        );

                        JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(
                                new Function2<Long, Long, Long>() {
                                    @Override
                                    public Long call(Long v1, Long v2) throws Exception {
                                        return v1 + v2;
                                    }
                                }
                        );

                        // 将dailyAdClickCountByProvinceRDD转换为DataFrame，注册为一张临时表
                        // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
                        JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(
                                new Function<Tuple2<String, Long>, Row>() {
                                    @Override
                                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                                        String[] keySplited = tuple._1.split("_");
                                        String datekey = keySplited[0];
                                        String date = DateUtils.formatDate(DateUtils.parseDateKey(datekey));
                                        String province = keySplited[1];
                                        long adid = Long.valueOf(keySplited[2]);
                                        long clickCount = tuple._2;

                                        return RowFactory.create(date, province, adid, clickCount);
                                    }
                                }
                        );
                        StructType schema = DataTypes.createStructType(
                                Arrays.asList(
                                        DataTypes.createStructField("date", DataTypes.StringType, true),
                                        DataTypes.createStructField("province", DataTypes.StringType, true),
                                        DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                        DataTypes.createStructField("click_count", DataTypes.LongType, true)
                                )
                        );
                        Dataset dailyAdClickCountByProvinceDF = ss.createDataFrame(rowsRDD, schema);

                        // 将dailyAdClickCountByProvinceDF，注册成一张临时表
                        dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");

                        // 使用Spark SQL执行SQL，配合开窗函数，统计出各身份top3热门的广告
                        Dataset provinceTop3AdDF = ss.sql(
                                "SELECT " +
                                            "date," +
                                            "province," +
                                            "ad_id," +
                                            "click_count " +
                                        "FROM (" +
                                            "SELECT " +
                                                "date," +
                                                "province," +
                                                "ad_id," +
                                                "click_count," +
                                                "row_number() OVER(PARTITION BY province ORDER BY click_count DESC) rank " +
                                            "FROM tmp_daily_ad_click_count_by_prov" +
                                        ") t" +
                                        "WHERE rank<=3"
                        );


                        return provinceTop3AdDF.javaRDD();
                    }
                }
        );

        // rowsDStream
        // 每次都是刷新出来各个省份最热门的top3广告
        // 将其中的数据批量更新到MySQL中
        rowsDStream.foreachRDD(
                new VoidFunction<JavaRDD<Row>>() {
                    @Override
                    public void call(JavaRDD<Row> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Row>>() {
                                    @Override
                                    public void call(Iterator<Row> iterator) throws Exception {
                                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<>();

                                        while(iterator.hasNext()) {
                                            Row row = iterator.next();
                                            String date = row.getString(0);
                                            String province = row.getString(1);
                                            long adId = row.getLong(2);
                                            long clickCount = row.getLong(3);

                                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                                            adProvinceTop3.setDate(date);
                                            adProvinceTop3.setProvince(province);
                                            adProvinceTop3.setAdId(adId);
                                            adProvinceTop3.setClickCount(clickCount);

                                            adProvinceTop3s.add(adProvinceTop3);
                                        }
                                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                                        adProvinceTop3DAO.insertBatch(adProvinceTop3s);
                                    }
                                }
                        );
                    }
                }
        );
    }

    /**
     * 每天各省各城市各广告点击量
     *
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String timeStamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timeStamp));
                        String dateKey = DateUtils.formatDateKey(date);//yyMMdd

                        String province = logSplited[1];
                        String city = logSplited[2];
                        long adId = Long.valueOf(logSplited[4]);

                        String key = dateKey + "_" + province + "_" + city + "_" + adId;

                        return new Tuple2<String, Long>(key, 1L);
                    }
                }
        );

        JavaPairDStream<String, Long> aggeregateDStream = mappedDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {

                        long clickCount = 0L;

                        //首先根据optional的状态判断之前是否有值存在，如果有，就从之前的值累加
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        for (Long value : values) {
                            clickCount += value;
                        }

                        return Optional.of(clickCount);
                    }
                }
        );

        //讲最新的结果同步到mysql中，便于j2ee系统使用
        aggeregateDStream.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        List<AdStat> adStats = new ArrayList<>();

                                        while (iterator.hasNext()) {
                                            Tuple2<String, Long> tuple = iterator.next();

                                            String[] keySplited = tuple._1.split("_");
                                            String date = keySplited[0];
                                            String province = keySplited[1];
                                            String city = keySplited[2];
                                            long adId = Long.valueOf(keySplited[3]);
                                            long clickCount = tuple._2;

                                            AdStat adStat = new AdStat();
                                            adStat.setDate(date);
                                            adStat.setProvince(province);
                                            adStat.setCity(city);
                                            adStat.setAdId(adId);
                                            adStat.setClickCount(clickCount);

                                            adStats.add(adStat);
                                        }

                                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                                        adStatDAO.updateBatch(adStats);
                                    }
                                }
                        );
                    }
                }
        );

        return aggeregateDStream;
    }

    /**
     * 动态黑名单生成
     *
     * @param filteredAdRealTimeLogDStream 过滤后的日志
     */
    private static void generateDynamicBlackList(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 一条一条的实时日志
        // timestamp province city userid adid
        // 某个时间点 某个省份 某个城市 某个用户 某个广告

        // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量

        // 通过对原始实时日志的处理
        // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        // 从tuple中获取到每一条原始的实时日志
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        // 提取出日期（yyyyMMdd）、userid、adid
                        String timeStamp = logSplited[0];
                        Date date = new Date(Long.valueOf(timeStamp));
                        String datekey = DateUtils.formatDateKey(date);

                        long userId = Long.valueOf(logSplited[3]);
                        long adId = Long.valueOf(logSplited[4]);

                        // 拼接key
                        String key = datekey + "_" + userId + "_" + adId;

                        return new Tuple2<>(key, 1L);
                    }
                }
        );

        // 针对处理后的日志格式，执行reduceByKey算子即可
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        dailyUserAdClickCountDStream.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        // 对每个分区的数据就去获取一次连接对象
                                        // 每次都是从连接池中获取，而不是每次都创建
                                        // 写数据库操作，性能已经提到最高了
                                        List<AdUserClickCount> adUserClickCounts = new ArrayList<>();

                                        while (iterator.hasNext()) {
                                            Tuple2<String, Long> tuple = iterator.next();

                                            String[] keySplited = tuple._1.split("_");
                                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));

                                            long userId = Long.valueOf(keySplited[1]);
                                            long adId = Long.valueOf(keySplited[2]);
                                            long clickCount = tuple._2;

                                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                                            adUserClickCount.setDate(date);
                                            adUserClickCount.setUserId(userId);
                                            adUserClickCount.setAdId(adId);
                                            adUserClickCount.setClickCount(clickCount);

                                            adUserClickCounts.add(adUserClickCount);
                                        }

                                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                                    }
                                }
                        );
                    }
                }
        );


        // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
        // 从mysql中查询
        // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
        // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化

        // 对batch中的数据，去查询mysql中的点击次数，使用哪个dstream呢？
        // dailyUserAdClickCountDStream
        // 为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
        // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
        // 所以选用这个聚合后的dstream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量
        // 一石二鸟，一举两得
        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split("_");

                        // yyyyMMdd -> yyyy-MM-dd
                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                        long userId = Long.valueOf(keySplited[1]);
                        long adId = Long.valueOf(keySplited[2]);

                        // 从mysql中查询指定日期指定用户对指定广告的点击量
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userId, adId);

                        // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
                        // 那么就拉入黑名单，返回true
                        if (clickCount >= 100) {
                            return true;
                        }

                        return false;
                    }
                }
        );

        // blacklistDStream
        // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
        // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
        // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
        // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
        // 所以直接插入mysql即可

        // 我们有没有发现这里有一个小小的问题？
        // blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
        // 那么是不是会发生，插入重复的黑明单用户
        // 我们在插入前要进行去重
        // yyyyMMdd_userid_adid
        // 20151220_10001_10002 100
        // 20151220_10001_10003 100
        // 10001这个userid就重复了

        // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
                new Function<Tuple2<String, Long>, Long>() {
                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        String key = tuple._1;
                        String[] keySplited = key.split(" ");
                        Long userId = Long.valueOf(keySplited[1]);

                        return userId;
                    }
                }
        );

        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
                new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                }
        );

        // 到这一步为止，distinctBlacklistUseridDStream
        // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
        distinctBlacklistUseridDStream.foreachRDD(
                new VoidFunction<JavaRDD<Long>>() {
                    @Override
                    public void call(JavaRDD<Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Long>>() {
                                    @Override
                                    public void call(Iterator<Long> iterator) throws Exception {
                                        List<AdBlackList> adBlackLists = new ArrayList<>();

                                        while (iterator.hasNext()) {
                                            long userId = iterator.next();

                                            AdBlackList adBlackList = new AdBlackList();
                                            adBlackList.setUserId(userId);

                                            adBlackLists.add(adBlackList);
                                        }

                                        IAdBlackListDAO adBlackListDAO = DAOFactory.getAdBlackListDAO();
                                        adBlackListDAO.insertBatch(adBlackLists);
                                    }
                                }
                        );
                    }
                }
        );
        // 到此为止，我们其实已经实现了动态黑名单了

        // 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中


        // 2、依据上述计算出来的数据，对每个batch中的按date、userid、adid聚合的数据
        // 都要遍历一遍，查询一下，对应的累计的点击次数，如果超过了100，那么就认定为黑名单
        // 然后对黑名单用户进行去重，去重后，将黑名单用户，持久化插入到mysql中
        // 所以说mysql中的ad_blacklist表中的黑名单用户，就是动态地实时地增长的
        // 所以说，mysql中的ad_blacklist表，就可以认为是一张动态黑名单

        // 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
        // 根据动态黑名单进行过滤
        // 把黑名单中的用户的点击行为，直接过滤掉

        // 动态黑名单机制，就完成了
    }


    /**
     * 通过与MySQL中的黑名单对比，返回值为不在黑名单中的
     *
     * @param adRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, String> filterByBlackList(
            JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志之后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        //首先从MySql中查询所有黑名单用户，将其转化为一个rdd
                        IAdBlackListDAO adBlackListDAO = DAOFactory.getAdBlackListDAO();
                        List<AdBlackList> adBlackLists = adBlackListDAO.findAllBlackUser();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                        //TODO:增强for循环应该有null判断，这里是自己增加的
                        if (adBlackLists != null) {
                            for (AdBlackList adBlackList : adBlackLists) {
                                tuples.add(new Tuple2<Long, Boolean>(adBlackList.getUserId(), true));
                            }
                        }

                        JavaSparkContext jsc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = jsc.parallelizePairs(tuples);

                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                    @Override
                                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                                        String log = tuple._2;
                                        String[] logSplited = log.split(" ");
                                        long userId = Long.valueOf(logSplited[3]);

                                        return new Tuple2<Long, Tuple2<String, String>>(userId, tuple);
                                    }
                                }
                        );

                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
                        // 用inner join，内连接，会导致数据丢失
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean
                                >>> filterRDD = joinedRDD.filter(
                                new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                    @Override
                                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        Optional<Boolean> optional = tuple._2._2;
                                        if (optional.isPresent() && optional.get()) {
                                            return false;
                                        }
                                        return true;
                                    }
                                }
                        );

                        JavaPairRDD<String, String> resultRDD = filterRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    @Override
                                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        return tuple._2._1;
                                    }
                                }
                        );

                        return resultRDD;
                    }
                }
        );

        return filteredAdRealTimeLogDStream;
    }

}
