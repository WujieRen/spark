package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IPageSplitConvertRateDAO;
import com.rwj.offlineAnalysisPrj.domain.PageSplitConvertRate;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

/**
 * Created by renwujie on 2018/02/06 at 17:46
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "";
        Object[] param = new Object[]{pageSplitConvertRate.getTaskId(), pageSplitConvertRate.getConvertRate()};

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeUpdate(sql, param);
    }
}
