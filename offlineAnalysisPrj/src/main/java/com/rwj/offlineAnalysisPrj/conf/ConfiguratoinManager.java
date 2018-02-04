package com.rwj.offlineAnalysisPrj.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by renwujie on 2018/01/09 at 19:27
 */
public class ConfiguratoinManager {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = ConfiguratoinManager.class.getClassLoader().getResourceAsStream("my.properties");

            prop.load(in);
        } catch (Exception e) {
            System.out.println("加载配置文件出错");
        }
    }

    /**
     * 获取指定key的value
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 返回整数类型配置项
     * @param key
     * @return
     */
    public static Integer getIntValue(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * 返回Boolean类型的配置项
     * @param key
     * @return
     */
    public static Boolean getBooleanValue(String key) {
        String value = getProperty(key);
        System.out.println(value);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return false;
    }

    /**
     * 返回Long类型的配置项
     * @param key
     * @return
     */
    public static Long getLongValue(String key) {
        String value = getProperty(key);
        try {
            Long.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return 0L;
    }

}
