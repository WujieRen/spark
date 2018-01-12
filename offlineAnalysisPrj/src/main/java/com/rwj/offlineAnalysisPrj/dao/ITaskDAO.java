package com.rwj.offlineAnalysisPrj.dao;

import com.rwj.offlineAnalysisPrj.domain.Task;

/**
 * Created by renwujie on 2018/01/12 at 11:00
 *
 * 任务管理Dao接口
 */
public interface ITaskDAO {

    Task findById(long id);

}
