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
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            Integer.valueOf(value);
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
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            Boolean.valueOf(key);
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
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            Long.valueOf(value);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return 0L;
    }

}
