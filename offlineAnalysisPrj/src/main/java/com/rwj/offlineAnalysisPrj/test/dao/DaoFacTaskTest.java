package com.rwj.offlineAnalysisPrj.test.dao;

import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.dao.factory.DAOFactory;
import com.rwj.offlineAnalysisPrj.domain.Task;

/**
 * Created by renwujie on 2018/01/12 at 11:47
 *
 * TaskDAO测试
 */
public class DaoFacTaskTest {
    public static void main(String[] args){
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(1);
        System.out.println(task.getTaskName());
    }
}
