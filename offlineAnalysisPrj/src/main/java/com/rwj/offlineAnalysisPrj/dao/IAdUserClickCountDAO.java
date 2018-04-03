package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AdUserClickCount;

import java.util.List;

/**
 * Created by renwujie on 2018/04/02 at 15:44
 */
public interface IAdUserClickCountDAO {
    /**
     * 批量更新用户广告点击量
     * @param adUserClickCountList
     */
    void updateBatch(List<AdUserClickCount> adUserClickCountList);

    /**
     * 根据多个key查询用户广告点击量
     * @param date
     * @param userId
     * @param adId
     * @return
     */
    int findClickCountByMultiKey(String date, long userId, long adId);
}
