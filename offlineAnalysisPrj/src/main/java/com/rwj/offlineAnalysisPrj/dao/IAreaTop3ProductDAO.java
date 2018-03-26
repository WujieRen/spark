package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AreaTop3Product;

import java.util.List;

/**
 * Created by renwujie on 2018/03/26 at 14:39
 */
public interface IAreaTop3ProductDAO {
    void insertBatch(List<AreaTop3Product> areaTopsProducts);
}
