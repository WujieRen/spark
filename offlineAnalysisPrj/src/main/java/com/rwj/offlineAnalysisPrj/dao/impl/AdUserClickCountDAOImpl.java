package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAdUserClickCountDAO;
import com.rwj.offlineAnalysisPrj.domain.AdUserClickCount;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;
import com.rwj.offlineAnalysisPrj.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/04/02 at 15:46
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {

    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCountList) {
        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //首先对用户广告点击量进行分类，分成待插入和待更新的
        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

        String selectSQL = "SELECT count(*) FROM ad_user_click_count WHERE date=? AND user_id=? AND ad_id=?";
        Object[] selectParams = null;
        for(AdUserClickCount adUserClickCount : adUserClickCountList) {
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

            selectParams = new Object[]{
                    adUserClickCount.getDate()
                    ,adUserClickCount.getUserId()
                    ,adUserClickCount.getAdId()};

            jdbcHelper.executeQuery(selectSQL, selectParams, new JdbcHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if(rs.next()) {
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });

            int count = queryResult.getCount();

            if(count > 0) {
                updateAdUserClickCounts.add(adUserClickCount);
            } else {
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        //批量插入
        String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";

        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = new Object[]{
                adUserClickCount.getDate(),
                adUserClickCount.getUserId(),
                adUserClickCount.getAdId(),
                adUserClickCount.getClickCount()
            };
            insertParamsList.add(insertParams);
        }
        jdbcHelper.executeBatch(insertSQL, insertParamsList);

        //批量更新
        String updateSQL = "UPDATE ad_user_click_count SET click_count=? WHERE date=? AND user_id=? AND ad_id=?";

        List<Object[]> updateParamList = new ArrayList<Object[]>();
        for(AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
            Object[] updateParams = new Object[] {
                    adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId()
            };
            updateParamList.add(updateParams);
        }
        jdbcHelper.executeBatch(updateSQL, updateParamList);

    }

    @Override
    public int findClickCountByMultiKey(String date, long userId, long adId) {
        String sql = "SELECT click_count "
                + "FROM ad_user_click_count "
                + "WHERE date=? "
                + "AND user_id=? "
                + "AND ad_id=?";

        Object[] params = new Object[]{date, userId, adId};

        final   AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()) {
                    int count = rs.getInt(1);
                    queryResult.setClickCount(count);
                }
            }
        });

        int clickCount = queryResult.getClickCount();

        return clickCount;
    }
}
