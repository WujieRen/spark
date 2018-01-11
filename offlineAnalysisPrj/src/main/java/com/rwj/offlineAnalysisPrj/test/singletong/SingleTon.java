package com.rwj.offlineAnalysisPrj.test.singletong;

/**
 * Created by renwujie on 2018/01/11 at 15:29
 *
 * 单例模式Demo
 *
 * 单例模式是指：当不希望外界随意创建 xxx.类 的实例，在整个程序运行期间，只有一个实例。
 *
 * 要实现单例模式，几个要点：
 *   1、如果不希望外界随意创建该类的对象，那么 Constructor 必须是 private 的。
 *   2、既然 Constructor 被 private 了，那么外界就只能通过 static 的方法去获取。
 *      所以类必须提供要给 static function， 通常是 getInstance()，
 *   3、且 getInstance() 能够保证类的实例只能被创建一次，返回唯一的一个实例。
 *
 * 单例模式应用场景：
 *   1、配置管理组件。可以在读取大量配置信息后，用单例模式，将配置信息仅仅保存在一个实例变量中，全局就只有一个实例。
 *      这样可以避免对于静态不变的配置信息的反复读取。
 *   2、JDBC辅助组件。全局只有一个实例，实例中持有一个简单的内部数据源。
 *      单例模式可以保证全局只有一个实例，那么数据源也只有一个，这样就不会重复创建数据源了(数据库连接池)。
 */
public class SingleTon {

    private static SingleTon instance = null;

    private SingleTon() {}

    public static SingleTon getInstance() {
        if(instance == null) {
            synchronized (SingleTon.class) {
                if(instance == null) {
                    instance = new SingleTon();
                }
            }
        }
        return instance;
    }

    //下面这个方法， 当第一次调用完成创建实例后， 在以后多个线程并发访问这个方法时，会在方法级别进行同步，导致并发性能大幅度降低。
    //TODO:我的疑问？？？方法级别的同步和块级别的同步哪个更占资源???
    //TODO:http://blog.csdn.net/happy_develop_/article/details/56489504
    /*public static synchronized SingleTon getINstance() {
        if(instance==null) {
            instance = new SingleTon();
        }
        return instance;
    }*/
}
