package com.rwj.offlineAnalysisPrj.test;

import com.rwj.offlineAnalysisPrj.jdbc.JdbcHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by renwujie on 2018/01/12 at 9:48
 */
public class JdbcHelperTest {
    public static void main(String[] args){

        JdbcHelper jdbcHelper = JdbcHelper.getInstance();

        /**
         * executeUpdate()
         */
        jdbcHelper.executeUpdate("insert into testjdbc(id, name, age, sex) values(?, ?, ?, ?)", new Object[]{19, "任武杰", 23, "男"});
        System.out.println("添加成功");

        jdbcHelper.executeUpdate("delete from testjdbc where id=9", null);
        System.out.println("executeUpdate成功");

        /**
         * 设计内部接口QueryCallback的用意：
         *  1.在执行sql语句时，可以封装和指定自己查询结果的处理逻辑
         *  2.封装在一个内部接口的匿名内部类对象中，传入JdbcHelper的方法，
         *  3.在方法内部，可以回掉自定义的逻辑，处理查询结果，并将结果放入外部的变量中
         */
        jdbcHelper.executeQuery("select * from testjdbc where id>=? and id <?", new Object[]{1, 5}, new JdbcHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while(rs.next()) {
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    int age = rs.getInt(3);
                    String sex = rs.getString(4);

                    System.out.println("id=" + id + ", name=" + name + ", age=" + age + ", sex=" + sex);
                }
            }
        });
        System.out.println("executeQuery()成功");

        /**
         * executeBatch()
         */
        String sql = "insert into testjdbc(id, name, age, sex) values(?, ?, ?, ?)";
        List<Object[]> paramList = new ArrayList<>();
        paramList.add(new Object[]{9, "杨亚芳", 25, "女"});
        paramList.add(new Object[]{10, "宋蓉蓉", 27, "女"});
        jdbcHelper.executeBatch(sql, paramList);
        System.out.println("executeBatch()测试成功");


    }
}
