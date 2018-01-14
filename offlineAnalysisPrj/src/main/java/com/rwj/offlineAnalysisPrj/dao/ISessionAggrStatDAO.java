package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.SessionAggrStat;

/**
 * Created by renwujie on 2018/01/14 at 21:29
 */
public interface ISessionAggrStatDAO {
    /**
     * 插入session聚合统计结果
     */
    void insert(SessionAggrStat sessionAggrStat);
}
