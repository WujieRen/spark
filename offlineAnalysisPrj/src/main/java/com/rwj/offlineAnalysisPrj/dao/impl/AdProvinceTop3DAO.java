package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAdProvinceTop3DAO;
import com.rwj.offlineAnalysisPrj.domain.AdProvinceTop3;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 12:43
 */
public class AdProvinceTop3DAO implements IAdProvinceTop3DAO {
    @Override
    public void insertBatch(List<AdProvinceTop3> adProvinceTop3s) {
        //先判断原先的表中是否已经存在相应日期中的某个省份，如果存在需要先删除再插入
        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        List<String> dateProvinces = new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();
            String key = date + "_" + province;

            if(dateProvinces.contains(key)) {
                dateProvinces.add(key);
            }
        }

        //// 根据去重后的date和province，进行批量删除操作
        String deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?";

        List<Object[]> deleteParams = new ArrayList<>();
        for(String dateProvince : dateProvinces) {
            String[] dateProvinceSplited = dateProvince.split("_");
            String date = dateProvinceSplited[0];
            String provine = dateProvinceSplited[1];

            Object[] params = new Object[]{date,provine};
            deleteParams.add(params);
        }
        jdbcHelper.executeBatch(deleteSQL, deleteParams);

        //更新数据库
        String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";

        List<Object[]> insertParams = new ArrayList<>();
        for(AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            Object[] params = new Object[]{
                    adProvinceTop3.getDate(),
                    adProvinceTop3.getProvince(),
                    adProvinceTop3.getAdId(),
                    adProvinceTop3.getClickCount()
            };
            insertParams.add(params);
        }
        jdbcHelper.executeBatch(insertSQL, insertParams);
    }
}
