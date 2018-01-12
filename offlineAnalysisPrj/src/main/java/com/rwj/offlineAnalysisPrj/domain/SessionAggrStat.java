package com.rwj.offlineAnalysisPrj.domain;

/**
 * Created by renwujie on 2017/11/10 at 12:27
 *
 * Session聚合统计
 */
public class SessionAggrStat {

    private long task_id;
    private long session_count;

    private double visit_length_1s_3s_ratio;
    private double visit_length_4s_6s_ratio;
    private double visit_length_7s_9s_ratio;
    private double visit_length_10s_30s_ratio;
    private double visit_length_30s_60s_ratio;
    private double visit_length_1m_3m_ratio;
    private double visit_length_3m_10m_ratio;
    private double visit_length_10m_30m_ratio;
    private double visit_length_30m_ratio;

    private double step_length_1_3;
    private double step_length_4_6;
    private double step_length_7_9;
    private double step_length_10_30;
    private double step_length_30_60;
    private double step_length_60;

    public long getTask_id() {
        return task_id;
    }

    public void setTask_id(long task_id) {
        this.task_id = task_id;
    }

    public long getSession_count() {
        return session_count;
    }

    public void setSession_count(long session_count) {
        this.session_count = session_count;
    }

    public double getVisit_length_1s_3s_ratio() {
        return visit_length_1s_3s_ratio;
    }

    public void setVisit_length_1s_3s_ratio(double visit_length_1s_3s_ratio) {
        this.visit_length_1s_3s_ratio = visit_length_1s_3s_ratio;
    }

    public double getVisit_length_4s_6s_ratio() {
        return visit_length_4s_6s_ratio;
    }

    public void setVisit_length_4s_6s_ratio(double visit_length_4s_6s_ratio) {
        this.visit_length_4s_6s_ratio = visit_length_4s_6s_ratio;
    }

    public double getVisit_length_7s_9s_ratio() {
        return visit_length_7s_9s_ratio;
    }

    public void setVisit_length_7s_9s_ratio(double visit_length_7s_9s_ratio) {
        this.visit_length_7s_9s_ratio = visit_length_7s_9s_ratio;
    }

    public double getVisit_length_10s_30s_ratio() {
        return visit_length_10s_30s_ratio;
    }

    public void setVisit_length_10s_30s_ratio(double visit_length_10s_30s_ratio) {
        this.visit_length_10s_30s_ratio = visit_length_10s_30s_ratio;
    }

    public double getVisit_length_30s_60s_ratio() {
        return visit_length_30s_60s_ratio;
    }

    public void setVisit_length_30s_60s_ratio(double visit_length_30s_60s_ratio) {
        this.visit_length_30s_60s_ratio = visit_length_30s_60s_ratio;
    }

    public double getVisit_length_1m_3m_ratio() {
        return visit_length_1m_3m_ratio;
    }

    public void setVisit_length_1m_3m_ratio(double visit_length_1m_3m_ratio) {
        this.visit_length_1m_3m_ratio = visit_length_1m_3m_ratio;
    }

    public double getVisit_length_3m_10m_ratio() {
        return visit_length_3m_10m_ratio;
    }

    public void setVisit_length_3m_10m_ratio(double visit_length_3m_10m_ratio) {
        this.visit_length_3m_10m_ratio = visit_length_3m_10m_ratio;
    }

    public double getVisit_length_10m_30m_ratio() {
        return visit_length_10m_30m_ratio;
    }

    public void setVisit_length_10m_30m_ratio(double visit_length_10m_30m_ratio) {
        this.visit_length_10m_30m_ratio = visit_length_10m_30m_ratio;
    }

    public double getVisit_length_30m_ratio() {
        return visit_length_30m_ratio;
    }

    public void setVisit_length_30m_ratio(double visit_length_30m_ratio) {
        this.visit_length_30m_ratio = visit_length_30m_ratio;
    }

    public double getStep_length_1_3() {
        return step_length_1_3;
    }

    public void setStep_length_1_3(double step_length_1_3) {
        this.step_length_1_3 = step_length_1_3;
    }

    public double getStep_length_4_6() {
        return step_length_4_6;
    }

    public void setStep_length_4_6(double step_length_4_6) {
        this.step_length_4_6 = step_length_4_6;
    }

    public double getStep_length_7_9() {
        return step_length_7_9;
    }

    public void setStep_length_7_9(double step_length_7_9) {
        this.step_length_7_9 = step_length_7_9;
    }

    public double getStep_length_10_30() {
        return step_length_10_30;
    }

    public void setStep_length_10_30(double step_length_10_30) {
        this.step_length_10_30 = step_length_10_30;
    }

    public double getStep_length_30_60() {
        return step_length_30_60;
    }

    public void setStep_length_30_60(double step_length_30_60) {
        this.step_length_30_60 = step_length_30_60;
    }

    public double getStep_length_60() {
        return step_length_60;
    }

    public void setStep_length_60(double step_length_60) {
        this.step_length_60 = step_length_60;
    }
}
