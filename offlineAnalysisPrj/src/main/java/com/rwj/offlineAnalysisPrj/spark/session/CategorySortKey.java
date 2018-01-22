package com.rwj.offlineAnalysisPrj.spark.session;

import scala.math.Ordered;

/**
 * Created by renwujie on 2018/01/22 at 23:02
 *
 * 二次排序
 *
 *  封装要进行排序的字段：点击次数、支付次数、下单次数。
 *  实现Order接口要求的几个方法。
 *  根其他key相比，如何来判定大于、大于等于、小于、小于等于
 *  依此使用三个参数进行比较，如果一个相等，那就比较下一个。
 *
 */
public class CategorySortKey implements Ordered<CategorySortKey> {

    private long clickCount;
    private long orderCount;
    private long payCount;

    public CategorySortKey(long clickCount, long orderCount, long payCount) {
        this.clickCount = clickCount;
        this.orderCount = orderCount;
        this.payCount = payCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    @Override
    public boolean $less(CategorySortKey that) {
        if(clickCount < that.clickCount) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount < that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount < that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategorySortKey that) {
        if(clickCount > that.getClickCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount > that.getOrderCount()) {
            return true;
        } else if (clickCount == that.getClickCount() && orderCount == that.getOrderCount() && payCount > that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategorySortKey that) {
        if($less(that)) {
            return true;
        } else if (clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategorySortKey that) {
        if($greater(that)) {
            return true;
        } else if (clickCount == that.getClickCount() &&
                orderCount == that.getOrderCount() &&
                payCount == that.getPayCount()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(CategorySortKey that) {
        if(clickCount - that.getClickCount() != 0) {
            return (int)(clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int)(orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int)(payCount - that.getPayCount());
        }
        return 0;
    }

    @Override
    public int compareTo(CategorySortKey that) {
        /*if(clickCount - that.getClickCount() != 0) {
            return (int)(clickCount - that.getClickCount());
        } else if (orderCount - that.getOrderCount() != 0) {
            return (int)(orderCount - that.getOrderCount());
        } else if (payCount - that.getPayCount() != 0) {
            return (int)(payCount - that.getPayCount());
        }
        return 0;*/
        if(this.$greater(that)) {
            return 1;
        } else if(this.$less(that)) {
            return -1;
        }
        return 0;
    }
}
