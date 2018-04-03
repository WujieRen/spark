package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAdBlackListDAO;
import com.rwj.offlineAnalysisPrj.domain.AdBlackList;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/04/02 at 17:20
 */
public class AdBlackListDAOImpl implements IAdBlackListDAO {
    @Override
    public void insertBatch(List<AdBlackList> adBlackListLists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList = new ArrayList<>();

        for(AdBlackList adBlackList : adBlackListLists) {
            Object[] params = new Object[]{adBlackList.getUserId()};
            paramsList.add(params);
        }

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    @Override
    public List<AdBlackList> findAllBlackUser() {
        String sql = "SELECT * FROM ad_blacklist";

        /**
         * 这儿又有疑问，final修饰的不是不可变么？
         *
         * 可是如果要再回调函数中用外部变量就得修饰成final，为啥？
         */
        final List<AdBlackList> adBlacklists = new ArrayList<>();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        jdbcHelper.executeQuery(sql, null, new JdbcHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long userId = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AdBlackList dadBlackList = new AdBlackList();
                    dadBlackList.setUserId(userId);

                    adBlacklists.add(dadBlackList);
                }
            }
        });

        return adBlacklists;
    }
}
