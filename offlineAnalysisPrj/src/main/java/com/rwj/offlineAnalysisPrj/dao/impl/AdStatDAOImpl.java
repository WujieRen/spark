package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.IAdStatDAO;
import com.rwj.offlineAnalysisPrj.domain.AdStat;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;
import com.rwj.offlineAnalysisPrj.model.AdStatQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/04/03 at 11:45
 */
public class AdStatDAOImpl implements IAdStatDAO{
    @Override
    public void updateBatch(List<AdStat> adStatList) {
        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        //区分开哪些是要插入的，哪些是要更新的
        List<AdStat> insertAdStats = new ArrayList<AdStat>();
        List<AdStat> updateAdStats = new ArrayList<AdStat>();

        String selectSQL = "SELECT count(*) "
                + "FROM ad_stat "
                + "WHERE date=? "
                + "AND province=? "
                + "AND city=? "
                + "AND ad_id=?";

        for(AdStat adStat : adStatList) {
            final AdStatQueryResult queryResult = new AdStatQueryResult();

            Object[] params = new Object[] {
                adStat.getDate(),
                adStat.getProvince(),
                adStat.getCity(),
                adStat.getAdId()
            };

            jdbcHelper.executeQuery(selectSQL, params, new JdbcHelper.QueryCallback() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    int count = rs.getInt(1);
                    queryResult.setCount(count);
                }
            });

            int count = queryResult.getCount();
            if(count > 0) {
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }

        // 对于需要插入的数据，执行批量插入操作
        String insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";

        List<Object[]> insertParamsList = new ArrayList<>();
        for(AdStat adStat : insertAdStats) {
            Object[] params = new Object[]{
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId(),
                    adStat.getClickCount()
            };
            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL, insertParamsList);

        //更新
        String updateSQL = "UPDATE ad_stat SET click_count=? "
                + "FROM ad_stat "
                + "WHERE date=? "
                + "AND province=? "
                + "AND city=? "
                + "AND ad_id=?";

        List<Object[]> updateParamsList = new ArrayList<>();
        for(AdStat adStat : updateAdStats) {
            Object[] params = new Object[] {
                    adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId()
            };
            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL, updateParamsList);
    }
}
