package com.rwj.offlineAnalysisPrj.test.singletong.threadExam1;

/**
 * Created by renwujie on 2018/01/11 at 15:53
 */
public class SynObj {

    public synchronized void A() {
        System.out.println("A...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void B() {
        synchronized (this) {
            System.out.println("B...");
        }
    }

    public void C() {
        String str = "ss";
        synchronized (str) {
            System.out.println("C...");
        }
    }
}
