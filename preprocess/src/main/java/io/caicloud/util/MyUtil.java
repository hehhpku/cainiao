package io.caicloud.util;

import org.apache.commons.lang.time.DateUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * MyUtil.java
 *
 * @author hehuihui@meituan.com
 * @date 2016-05-20
 * @brief
 */


public class MyUtil {

    public static Long getNearDay(Long day, int amount) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date = df.parse(String.valueOf(day));
            Date nearDate = DateUtils.addDays(date, amount);
            Long nearDay = Long.parseLong(df.format(nearDate));
            return nearDay;
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }


    public static double parseDouble(Object obj) {
        if (obj == null) {
            return 0d;
        }

        try {
            double d = Double.parseDouble(obj.toString());
            return d;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0d;
    }

    public static long parseLong(Object obj) {
        if (obj == null) {
            return 0L;
        }

        try {
            long value = Long.parseLong(obj.toString());
            return value;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public static double round(double v) {
        double x = 100.0;
        double value = Math.round(v * x) / x;
        return value;
    }

    public static double calcPrice(double amount, double qty) {
        if (qty < 1e-1) {
            return 0d;
        }
        double price = amount / Math.max(1, qty);
        return MyUtil.round(price);
    }

    public static double calcRatio(double x, double y) {
        y = Math.max(1, y);
//        double ratio = Math.min(1, x / y);
        double ratio = x / y;
        return MyUtil.round(ratio * 100);
    }

    public static double calcTrend(double x, double y) {
        double ratio = 0;
        if (y < 1) {
            return ratio;
        }

        if (x < y) {
            ratio = 1 - x / y;
        } else {
            ratio = y / x - 1;
        }
        return MyUtil.round(ratio * 100);
    }

    public static void main(String[] args) {
        Long day = 20160520L;
        for (int i = -14; i < 14; i++) {
            Long nearDay = getNearDay(day, i);
            System.out.println("i = " + i + "; day = " + nearDay);
        }
    }
}
