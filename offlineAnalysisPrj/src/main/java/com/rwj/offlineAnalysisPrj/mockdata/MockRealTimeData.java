package com.rwj.offlineAnalysisPrj.mockdata;

import com.rwj.offlineAnalysisPrj.conf.ConfigurationManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.*;

/**
 * Created by renwujie on 2018/04/15 at 16:47
 */
public class MockRealTimeData extends Thread {

    private static final Random random = new Random();
    private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei", "Sichuan", "Shanxi"};
    private static final Map<String, String[]> provinceCityMap = new HashMap<>();


    private Producer<Integer, String>  producer;

    public MockRealTimeData() {
        provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou", "Wuxi", "Yancheng", "Yangzhou"});
        provinceCityMap.put("Sichuan", new String[] {"Chengdu", "Neijiang", "Mianyang", "Guangyuan", "Zigong"});
        provinceCityMap.put("Shanxi", new String[] {"Xian", "Baoji", "Ankang", "Weinan", "Hanzhong"});
        provinceCityMap.put("Hubei", new String[] {"Wuhan", "Yichang", "Jingzhou", "Huanggang", "Xiaogan"});
        provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan", "Shaoyang", "Changde", "Huaihua"});
        provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang", "Xinxiang", "Anyang", "Kaifeng"});
        provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Tangshan", "Baoding", "Cangzhou", "Qinhuangdao"});

        producer = new Producer<Integer, String>(createProducerConfig());
    }

    private ProducerConfig createProducerConfig() {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "sparkproject1:9092,sparkproject2:9092,sparkproject3:9092");
        return new ProducerConfig(props);
    }

    public void run() {
        while(true) {
            String province = provinces[random.nextInt(7)];
            String city = provinceCityMap.get(province)[random.nextInt(5)];

            String log = new Date().getTime() + " " + province + " " + city + " " + random.nextInt(100) + " " + random.nextInt(10);
            producer.send(new KeyedMessage<Integer, String>("AdRealTimeLog", log));

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args){
        MockRealTimeData producer = new MockRealTimeData();
        producer.start();
    }

}
