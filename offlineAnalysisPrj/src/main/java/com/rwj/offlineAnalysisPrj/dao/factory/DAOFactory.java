package com.rwj.offlineAnalysisPrj.dao.factory;

import com.rwj.offlineAnalysisPrj.dao.ISessionAggrStatDAO;
import com.rwj.offlineAnalysisPrj.dao.ISessionRandomExtractDAO;
import com.rwj.offlineAnalysisPrj.dao.ITaskDAO;
import com.rwj.offlineAnalysisPrj.dao.impl.SessionAggrStatDAOImpl;
import com.rwj.offlineAnalysisPrj.dao.impl.SessionRandomExtractDAOImpl;
import com.rwj.offlineAnalysisPrj.dao.impl.TaskDAOImpl;

/**
 * Created by renwujie on 2018/01/12 at 11:46
 */
public class DAOFactory {

    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getsessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

}
