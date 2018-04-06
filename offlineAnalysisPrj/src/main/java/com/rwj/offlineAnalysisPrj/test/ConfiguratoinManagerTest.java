package com.rwj.offlineAnalysisPrj.test;

import com.rwj.offlineAnalysisPrj.conf.ConfigurationManager;

/**
 * Created by renwujie on 2018/01/11 at 15:23
 */
public class ConfiguratoinManagerTest {
    public static void main(String[] args){
        System.out.println(ConfigurationManager.getProperty("jdbc.driver"));
    }
}
