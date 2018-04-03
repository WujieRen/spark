package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AdClickTrend;

import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 13:26
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrendList);
}
