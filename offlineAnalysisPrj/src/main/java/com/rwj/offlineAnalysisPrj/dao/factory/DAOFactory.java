package com.rwj.offlineAnalysisPrj.dao.factory;

import com.rwj.offlineAnalysisPrj.dao.*;
import com.rwj.offlineAnalysisPrj.dao.impl.*;

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

    public static ISessionDetailDAO getSessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }
}
