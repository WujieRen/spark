package com.rwj.offlineAnalysisPrj.dao.impl;

import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.domain.Task;
import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

import java.sql.ResultSet;

/**
 * Created by renwujie on 2018/01/12 at 11:28
 */
public class TaskDAOImpl implements ITaskDAO {

    /**
     * 根据id(主键)查询任务
     *
     * @param taskId taskId
     * @return new Task()
     */
    @Override
    public Task findById(long taskId) {

        final Task task = new Task();

        String sql = "select * from task where task_id=?";
        Object[] params = new Object[]{taskId};

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });
        return task;
    }
}
