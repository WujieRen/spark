package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.ITop10SessionDAO;
import com.rwj.offlineAnalysisPrj.domain.Top10Session;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

/**
 * Created by renwujie on 2018/01/25 at 16:26
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO{

    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";
        Object[] param = new Object[]{
                top10Session.getTaskId(),
                top10Session.getCategoryId(),
                top10Session.getSessionId(),
                top10Session.getClickCount()
        };

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, param);
    }
}
