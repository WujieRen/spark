package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AdStat;

import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 11:44
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStatList);
}
