package io.caicloud.util;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.google.common.collect.Maps;
import org.apache.commons.lang.time.DateUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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

    public static double round(double v) {
        double x = 100.0;
        double value = Math.round(v * x) / x;
        return value;
    }

    public static void main(String[] args) {
        Long day = 20160520L;
        for (int i = -14; i < 14; i++) {
            Long nearDay = getNearDay(day, i);
            System.out.println("i = " + i + "; day = " + nearDay);
        }
    }
}
