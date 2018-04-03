package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAdClickTrendDAO;
import com.rwj.offlineAnalysisPrj.domain.AdClickTrend;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;
import com.rwj.offlineAnalysisPrj.model.AdClickTrendQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 13:27
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrendList) {
        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //区分出来哪些数据是要插入的，哪些数据是要更新的
        // j2ee系统在查询的时候，直接查询最新的数据即可（规避掉重复插入的问题）
        List<AdClickTrend> updateAdClickTrends = new ArrayList<AdClickTrend>();
        List<AdClickTrend> insertAdClickTrends = new ArrayList<AdClickTrend>();

        String selectSQL = "SELECT count(*) "
                + "FROM ad_click_trend "
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";

        for(AdClickTrend adClickTrend : adClickTrendList) {
            final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();

            Object[] params = new Object[]{
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid()
            };

            jdbcHelper.executeQuery(selectSQL, params, new JdbcHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    queryResult.setCount(rs.getInt(1));
                }
            });

            int count = queryResult.getCount();
            if(count > 0) {
                updateAdClickTrends.add(adClickTrend);
            } else {
                insertAdClickTrends.add(adClickTrend);
            }
        }

        //批量更新
        String updateSQL = "UPDATE ad_click_trend SET click_count=? "
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";

        List<Object[]> updateParams = new ArrayList<>();
        for(AdClickTrend adClickTrend : updateAdClickTrends) {
            Object[] params = new Object[]{
                    adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid()
            };
            updateParams.add(params);
        }
        jdbcHelper.executeBatch(updateSQL, updateParams);

        //批量插入
        String insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";

        List<Object[]> insertParams = new ArrayList<>();
        for(AdClickTrend adClickTrend : insertAdClickTrends) {
            Object[] params = new Object[] {
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdid(),
                    adClickTrend.getClickCount()
            };
            insertParams.add(params);
        }
        jdbcHelper.executeBatch(insertSQL, insertParams);
    }
}
