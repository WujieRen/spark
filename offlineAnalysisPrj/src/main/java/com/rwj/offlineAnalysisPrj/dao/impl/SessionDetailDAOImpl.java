package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.ISessionDetailDAO;
import com.rwj.offlineAnalysisPrj.domain.SessionDetail;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

/**
 * Created by renwujie on 2018/01/19 at 11:28
 */
public class SessionDetailDAOImpl implements ISessionDetailDAO {
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";
        Object[] param = new Object[]{
                sessionDetail.getTaskId(),
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, param);
    }
}
