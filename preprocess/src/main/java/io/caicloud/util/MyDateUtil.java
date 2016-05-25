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
    public final long[] PROMOTION_DAYS = {20141111, 20141212, 20150614, 20150618, 20151111, 20151212};

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

        for (int j = 0; j < PROMOTION_DAYS.length; j++) {
            for (int k = -7; k < 2; k++) {
                Long nextDay = getNearDay(PROMOTION_DAYS[j], k);
                promotionDayMap.put(nextDay, 0);
            }
        }
    }

    public Map<Long, Integer> getDayMap() {
        return dayMap;
    }

    public List<Long> getDayList() {
        return dayList;
    }

    public Map<Long, Integer> getPromotionDayMap() {
        return promotionDayMap;
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

    public static void main(String[] args) {
//        System.out.println(MyDateUtil.dayList);
//        for (Long day : MyDateUtil.dayList) {
//            int index = MyDateUtil.dayIndexMap.get(day);
//            System.out.println("day=" + day + "; index=" + index);
//        }
    }
}
