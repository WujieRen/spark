package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.ISessionRandomExtractDAO;
import com.rwj.offlineAnalysisPrj.domain.SessionRandomExtract;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

/**
 * Created by renwujie on 2018/01/16 at 18:07
 */
public class SessionRandomExtractDAO implements ISessionRandomExtractDAO {

    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[]{
                sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        System.out.println(sessionRandomExtract.getSearchKeywords());

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);
    }
}
