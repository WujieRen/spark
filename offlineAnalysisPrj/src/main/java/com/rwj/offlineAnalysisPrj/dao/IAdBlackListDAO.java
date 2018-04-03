package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.AdBlackList;

import java.util.List;

/**
 * Created by renwujie on 2018/04/02 at 17:19
 */
public interface IAdBlackListDAO {
    /**
     * 批量插入广告黑名单客户
     * @param adBlackListList
     */
    void insertBatch(List<AdBlackList> adBlackListList);

    /**
     * 查询所有黑名单用户
     * @return
     */
    List<AdBlackList> findAllBlackUser();
}
