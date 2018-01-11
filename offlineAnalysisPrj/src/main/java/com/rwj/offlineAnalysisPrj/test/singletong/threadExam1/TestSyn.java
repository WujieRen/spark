package com.rwj.offlineAnalysisPrj.test.singletong.threadExam1;

/**
 * Created by renwujie on 2018/01/11 at 15:55
 */
public class TestSyn {

    public static void main(String[] args){

        SynObj synObj = new SynObj();

        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                synObj.A();
            }
        });
        t1.start();

        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                synObj.B();
            }
        });
        t2.start();

        Thread t3 = new Thread(new Runnable() {
            @Override
            public void run() {
                synObj.C();
            }
        });
        t3.start();
    }
}
