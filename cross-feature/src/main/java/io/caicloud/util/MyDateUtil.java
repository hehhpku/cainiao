package io.caicloud.util;

import org.apache.commons.lang.time.DateUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * MyDateUtil.java
 *
 * @author hehuihui@meituan.com
 * @date 2016-05-20
 * @brief
 */

public class MyDateUtil {

    public final long START_DAY = 20141001L;
    public final long END_DAY = 20151227L;
    public final long TRAIN_END_DAY = 20151025;
    public final List<Long> PROMOTION_DAYS = Arrays.asList(20141111L, 20141212L, 20150614L, 20150618L, 20151111L, 20151212L);
    public final List<Long> FESTIVAL_DAYS = Arrays.asList(20150101L, 20150404L, 20150501L, 20150903L, 20151001L, 20151002L, 20151003L, 20160101L);

    public Map<Long, Integer> dayMap = new HashMap<Long, Integer>();
    public Map<Long, Integer> promotionDayMap = new HashMap<Long, Integer>();
    public List<Long> dayList = new ArrayList<Long>();

    public MyDateUtil() {
        init();
    }

    /**
     * 初始化 dayIndexMap、dayList
     */
    public void init() {
        Long startDay = START_DAY;
        Long endDay = END_DAY;

        int i = 0;
        while (true) {
            Long nextDay = getNearDay(startDay, i);
            if (nextDay > endDay) {
                break;
            }
            dayMap.put(nextDay, i);
            dayList.add(nextDay);
            i++;
        }

        for (int j = 0; j < PROMOTION_DAYS.size(); j++) {
            for (int k = -7; k < 2; k++) {
                Long nextDay = getNearDay(PROMOTION_DAYS.get(j), k);
                promotionDayMap.put(nextDay, 0);
            }
        }
    }

    public long getDiffDate(Long day1, Long day2) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date1 = df.parse(String.valueOf(day1));
            Date date2 = df.parse(String.valueOf(day2));
            return (date1.getTime() - date2.getTime()) / (24 * 60 * 60 * 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public long getDiffDays(Long day1, Long day2) {
        if (dayMap.containsKey(day1) && dayMap.containsKey(day2)) {
            int index1 = dayMap.get(day1);
            int index2 = dayMap.get(day2);
            return Math.abs(index1 - index2);
        }
        return -1;
    }

    /**
     * 计算距当前日期第N天的日期
     * @param day 当前日期
     * @param amount 第N天
     * @return
     */
    public static Long getNearDay(Long day, int amount) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        try {
            Date date = df.parse(String.valueOf(day));
            Date nearDate = DateUtils.addDays(date, amount);
            Long nearDay = Long.parseLong(df.format(nearDate));
            return nearDay;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 计算距离当前日期第[start, end)天的日期列表
     * @param day
     * @param start
     * @param end
     * @return
     */
    public List<Long> getNearDayList(Long day, int start, int end) {
        List<Long> result = new ArrayList<Long>();

        Integer index = dayMap.get(day);
        if (index == null) {
            return result;
        }

        int startIndex = index + start;
        int endIndex = index + end;
        return dayList.subList(startIndex, endIndex);
    }

    public int getSeason(Long day) {
        if (day >= 20150301 && day < 20150501) {
            return 1;
        }
        if (day >= 20150501 && day < 20150901) {
            return 2;
        }
        if (day >= 20150901 && day < 20151101) {
            return 3;
        }
        return 0;
    }

    public static void main(String[] args) {
//        System.out.println(MyDateUtil.dayList);
//        for (Long day : MyDateUtil.dayList) {
//            int index = MyDateUtil.dayIndexMap.get(day);
//            System.out.println("day=" + day + "; index=" + index);
//        }
        Long day = 20151213L;
        MyDateUtil myDateUtil = new MyDateUtil();
        System.out.println(myDateUtil.getNearDayList(day, -13, 1));
        System.out.println(myDateUtil.getNearDayList(day, 1, 15));
    }
}
