package com.rwj.offlineAnalysisPrj.jdbc;

import com.rwj.offlineAnalysisPrj.conf.ConfiguratoinManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by renwujie on 2018/01/11 at 17:02
 */
public class JdbcHelper {

    //加载驱动
    static {

        try {
            String driver = ConfiguratoinManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            System.out.println("加载驱动出错");
        }
    }

    //private实例
    private static JdbcHelper instance = null;

    //提供public static 方法
    public static JdbcHelper getInstance() {
        if (instance == null) {
            synchronized (JdbcHelper.class) {
                if (instance == null) {
                    instance = new JdbcHelper();
                }
            }
        }
        return instance;
    }

    //数据库连接池
    private LinkedList<Connection> dataSource = new LinkedList<Connection>();

    //private Constructor && 将创建的 Connectin 放在唯一的数据库连接池中
    private JdbcHelper() {
        int dataSourceSize = ConfiguratoinManager.getIntValue(Constants.JDBC_DATASOURCE_SIZE);

        for (int i = 0; i < dataSourceSize; i++) {
            String url = ConfiguratoinManager.getProperty(Constants.JDBC_URL);
            String user = ConfiguratoinManager.getProperty(Constants.JDBC_USER);
            String password = ConfiguratoinManager.getProperty(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                dataSource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //TODO:什么时候应该考虑是否需要多线程锁???
    //获取 Connection，需要考虑并发
    public synchronized Connection getConnection() {
        //这里注意，我一开始写成if了，if显然是错误的。如果datasource没有了，这里是要求循环执行的。
        while (dataSource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //poll() —— Retrieves and removes the head (first element) of this list.
        return dataSource.poll();
    }

    /**
     * JdbcCRUD
     */
    //增删改
    public int executeUpdate(String sql, Object[] params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        int rtn = 0;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }

        return rtn;
    }

    //查
    public void executeQuery(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rs = pstmt.executeQuery();
            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
    }

    //批量执行SQL语句
    public int[] executeBatch(String sql, List<Object[]> paramList) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        int[] rtn = null;

        try {
            conn = getConnection();
            //第一步，取消Connection自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            if (paramList != null && paramList.size() > 0) {
                for (Object[] param : paramList) {
                    for (int i = 0; i < param.length; i++) {
                        pstmt.setObject(i + 1, param[i]);
                    }
                    //第二步，Adds a set of parameters to this <code>PreparedStatement</code> object's batch of commands.
                    pstmt.addBatch();
                }
            }

            rtn = pstmt.executeBatch();
            //最后一步，手动提交 批量的SQL语句
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rtn;
    }

    //匿名内部类：查询回掉接口
    //TODO:这块儿没太明白，看下原理
    public static interface QueryCallback {
        //处理查询结果
        void process(ResultSet rs) throws Exception;
    }

}
