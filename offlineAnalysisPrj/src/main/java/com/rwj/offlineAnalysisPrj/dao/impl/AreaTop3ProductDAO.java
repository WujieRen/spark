package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAreaTop3ProductDAO;
import com.rwj.offlineAnalysisPrj.domain.AreaTop3Product;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/03/26 at 14:39
 */
public class AreaTop3ProductDAO implements IAreaTop3ProductDAO{

    @Override
    public void insertBatch(List<AreaTop3Product> areaTopsProducts) {
        String sql = "insert into area_top3_product values(?,?,?,?,?,?,?,?)";
        List<Object[]> paramList = new ArrayList<>();

        for(AreaTop3Product areaTopsProduct : areaTopsProducts) {
            Object[] param = new Object[8];

            param[0] = areaTopsProduct.getTaskid();
            param[1] = areaTopsProduct.getArea();
            param[2] = areaTopsProduct.getAreaLevel();
            param[3] = areaTopsProduct.getProductid();
            param[4] = areaTopsProduct.getCityInfos();
            param[5] = areaTopsProduct.getClickCount();
            param[6] = areaTopsProduct.getProductName();
            param[7] = areaTopsProduct.getProductStatus();

            paramList.add(param);
        }

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeBatch(sql,paramList);
    }
}
