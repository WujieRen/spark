package com.rwj.offlineAnalysisPrj.test;

import java.sql.*;

/**
 * Created by renwujie on 2018/01/11 at 14:28
 *
 *  <p>
 * JDBCTest
 * <p>
 * 一、获取数据库连接对象
 * 1、加载数据库驱动类
 * 1、定义数据库连接对象,获取数据库连接对象
 * 2、基于数据库连接对象，创建SQL语句执行句柄【Statement对象】。定义sql语句执行句柄：Statement对象【底层基于Connection数据库连接对象，可以方便地对数据库中的表进行增删改查的操作】
 * 4、释放相应对象
 *
 *jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8
 *
 * com.mysql.jdbc.Driver
 */
public class JdbcCRUD {

    public static void main(String[] args){
        Connection conn = getConnection();
        Statement stmt = getStatement(conn);
        //insert(stmt);
        //update(stmt);
        delete(stmt);
        //select(stmt);
        preInsert(conn);
        close(conn, stmt);
    }

    public static void insert(Statement stmt) {

        try {

            String sql = "INSERT INTO testjdbc(id, name, age, sex) VALUES(21, 'reviewSparkPrj', 25, '男') ";
            int rn = stmt.executeUpdate(sql);

            System.out.println("sql语句影响了【" + rn + "】行");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void update(Statement stmt) {

        try {

            String sql = "update testjdbc set age = 0 where id=21";
            int rn = stmt.executeUpdate(sql);

            System.out.println("执行影响了【" + rn + "行】");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void delete(Statement stmt) {

        try {
            String sql = "delete from testjdbc where age = 0";
            int rn = stmt.executeUpdate(sql);

            System.out.println("影响了【" + rn + "】");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void select(Statement stmt) {

        try {
            String sql = "select * from testjdbc";
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                int age = rs.getInt(3);
                String sex = rs.getString(4);

                System.out.println("id=" + id + "，name=" + name + "，age=" + age + ",sex=" + sex);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void preInsert(Connection conn) {

        PreparedStatement pstmt = null;

        try {

            String sql = "INSERT INTO testjdbc(id, name, age, sex) VALUES(?, ?, ?, ?)";
            pstmt = conn.prepareStatement(sql);

            pstmt.setInt(1, 8);
            pstmt.setString(2, "Pstmt");
            pstmt.setInt(3, 0);
            pstmt.setString(4,"无");

            int rn = pstmt.executeUpdate();
            System.out.println("执行影响了【" + rn + "】行");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(pstmt != null) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取Connection对象
     * @return
     */
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/testdata?characterEncoding=utf8", "root", "root");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取Statement对象
     * @param conn
     * @return
     */
    public static Statement getStatement(Connection conn) {
        try {
            return conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭资源
     * @param conn
     * @param stmt
     */
    public static void close(Connection conn, Statement stmt) {
        try {
            if(conn != null) {
                conn.close();
            }

            if(stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
