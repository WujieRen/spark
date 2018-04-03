package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AdProvinceTop3;

import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 12:39
 */
public interface IAdProvinceTop3DAO {
    void insertBatch(List<AdProvinceTop3> adProvinceTop3s);
}
