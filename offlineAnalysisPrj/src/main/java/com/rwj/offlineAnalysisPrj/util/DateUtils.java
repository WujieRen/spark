package com.rwj.offlineAnalysisPrj.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by renwujie on 2018/01/05 at 14:07
 * <p>
 * 时间日期工具类
 */
public class DateUtils {
    public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
    public static final String str = "";

    /**
     * 判断一个时间是否在另一个时间之前
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean before(String time1, String time2) {
        try {
            synchronized (str) {
                Date dateTime1 = TIME_FORMAT.parse(time1);
                Date dateTime2 = TIME_FORMAT.parse(time2);

                if (dateTime1.before(dateTime2)) {
                    return true;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断一个时间是否在另一个时间之后
     *
     * @param time1 第一个时间
     * @param time2 第二个时间
     * @return 判断结果
     */
    public static boolean after(String time1, String time2) {
        try {
            synchronized (str) {
                Date dateTime1 = TIME_FORMAT.parse(time1);
                Date dateTime2 = TIME_FORMAT.parse(time2);
                if (dateTime1.after(dateTime2)) {
                    return true;
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 计算时间差值（单位为秒）
     *
     * @param time1 时间1
     * @param time2 时间2
     * @return 差值
     */
    public static int minus(String time1, String time2) {
        try {
            synchronized (str) {
                Date dateTime1 = TIME_FORMAT.parse(time1);
                Date dateTime2 = TIME_FORMAT.parse(time2);

                long millisecond = dateTime1.getTime() - dateTime2.getTime();

                return Integer.valueOf(String.valueOf(millisecond / 1000));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 获取年月日和小时
     *
     * @param dateTime 时间（yyyy-MM-dd HH:mm:ss）
     * @return 结果（yyyy-MM-dd_HH）
     */
    public static String getDateHour(String dateTime) {
        String[] times = dateTime.split(" ");
        String ymd = times[0];
        String h = times[1].split(":")[0];
        return ymd + "_" + h;
    }

    /**
     * 获取当天日期（yyyy-MM-dd）
     *
     * @return 当天日期
     */
    public static String getTodayDate() {
        return DATE_FORMAT.format(new Date());
    }

    /**
     * 获取昨天的日期（yyyy-MM-dd）
     *
     * @return 昨天的日期
     */
    public static String getYesterdayDate() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_YEAR, -1);

        Date date = calendar.getTime();

        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化日期（yyyy-MM-dd）
     *
     * @param date Date对象
     * @return 格式化后的日期
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * 格式化时间（yyyy-MM-dd HH:mm:ss）
     *
     * @param date Date对象
     * @return 格式化后的时间
     */
    public static String formatTime(Date date) {
        //final SimpleDateFormat TIME_FORMATS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        synchronized (str) {
            return TIME_FORMAT.format(date);
        }
    }

    /**
     * 解析时间字符串
     *
     * @param time 时间字符串
     *             2018-01-17 17:56:50
     * @return Date
     */
    public static Date parseTime(String time) {
        try {
            String str = "";
            synchronized (str) {
                return TIME_FORMAT.parse(time);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 格式化日期key
     *  yyMMdd
     * @param date
     * @return
     */
    public static String formatDateKey(Date date) {
        return DATEKEY_FORMAT.format(date);
    }

    /**
     * 格式化日期key
     * @param dateKey
     * @return
     */
    public static Date parseDateKey(String dateKey) {
        try {
            return DATEKEY_FORMAT.parse(dateKey);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 格式化时间，保留到分钟级别
     * yyyyMMddHHmm
     *
     * @param date
     * @return
     */
    public static String formatTimeMinute(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
        return sdf.format(date);
    }

    /*public static void main(String[] args) throws ParseException {
        TimeUnit.MICROSECONDS.toDays(100);
        for (int i = 0 ; i < 10 ; i ++ ) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    //SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date d = null;
                    try {
                        d = TIME_FORMAT.parse("2018-01-17 17:56:50");
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    System.out.println(Thread.currentThread().getName()+"--->"+d);

                }
            }).start();
        }
    }*/

   /* public static void main(String[] args) {
        Date d = new Date("2018-01-17 17:56:50");

    }*/
}
