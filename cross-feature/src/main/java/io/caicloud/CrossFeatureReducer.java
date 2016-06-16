package io.caicloud;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.ReducerBase;
import io.caicloud.util.MyDateUtil;
import io.caicloud.util.MyUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * CrossFeatureReducer.java
 *
 * @author hehuihui@meituan.com
 * @date 2016-05-20
 * @brief
 */


public class CrossFeatureReducer extends ReducerBase {
    private Record result;
    private final int[] intervals = {1, 3, 7, 14, 28, 56};
    private MyDateUtil myDateUtil = new MyDateUtil();

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Long startDay = myDateUtil.END_DAY;
        Map<Long, Object[]> map = new HashMap<Long, Object[]>();
        Long saleSum = 0L;
        Long saleMax = 0L;
        Long saleMin = 100000000L;
        int N = 0;
        while (values.hasNext()) {
            Record val = values.next();
            Long day = val.getBigint(1);
            if (startDay > day) {
                startDay = day;
            }
            map.put(day, val.toArray());
            Long sale = val.getBigint(0);
            saleSum += sale;
            saleMax = Math.max(saleMax, sale);
            saleMin = Math.min(saleMin, sale);
            N++;
        }
        double avgSale = MyUtil.round(saleSum / (Math.max(N, 1) * 14.0));

        for (Map.Entry<Long, Object[]> entry : map.entrySet()) {
            Long day = entry.getKey();
            if (myDateUtil.getDiffDays(day, startDay) < 28 && startDay < myDateUtil.TRAIN_END_DAY) {
                continue;
            }
            Object[] recordValue = entry.getValue();

            int index = -1;

            // sale_sum,thedate,item_id,store_code,cate,level,brand,supplier
            for (int i = 0; i < 22; i++) {
                result.set(++index, recordValue[i]);
            }

            // day01~day14
//            for (int i = 14; i < 28; i++) {
//                Long nearday = MyUtil.getNearDay(day, i);
//                Object[] nearRecord = map.get(nearday);
//                long sales = 0;
//                if (nearRecord != null) {
//                    sales = MyUtil.parseLong(nearRecord[0]) / 14;
//                }
//                result.set(++index, sales);
//            }

            double[][] columns = new double[intervals.length][24];

            // 计算价格特征、比率特征
            for (int i = 0; i < intervals.length; i++) {
                int t = 22 + i * 25;
                double pv_ipv = MyUtil.parseDouble(recordValue[t]);         // pv
                double pv_uv = MyUtil.parseDouble(recordValue[t + 1]);      // uv
                double cart_ipv = MyUtil.parseDouble(recordValue[t + 2]);   // 加购
                double collect_uv = MyUtil.parseDouble(recordValue[t + 4]); // 收藏

                double amt_gmv = MyUtil.parseDouble(recordValue[t + 6]);  // 拍下
                double qty_gmv = MyUtil.parseDouble(recordValue[t + 7]);  // 拍下件数
                double price_gmv = MyUtil.calcPrice(amt_gmv, qty_gmv);

                double amt_alipay = MyUtil.parseDouble(recordValue[t + 9]); // 支付
                double qty_alipay = MyUtil.parseDouble(recordValue[t + 11]);
                double unum_alipay = MyUtil.parseDouble(recordValue[t + 12]);
                double price_alipay = MyUtil.calcPrice(amt_alipay, qty_alipay);

                double ztc_pv_ipv = MyUtil.parseDouble(recordValue[t + 13]);
                double tbk_pv_ipv = MyUtil.parseDouble(recordValue[t + 14]);
                double ss_pv_ipv = MyUtil.parseDouble(recordValue[t + 15]);
                double jhs_pv_ipv = MyUtil.parseDouble(recordValue[t + 16]);
                double pv_sum = ztc_pv_ipv + tbk_pv_ipv + ss_pv_ipv + jhs_pv_ipv;
                double ztc_pv_ratio = MyUtil.calcRatio(ztc_pv_ipv, pv_sum);
                double tbk_pv_ratio = MyUtil.calcRatio(tbk_pv_ipv, pv_sum);
                double ss_pv_ratio = MyUtil.calcRatio(ss_pv_ipv, pv_sum);
                double jhs_pv_ratio = MyUtil.calcRatio(jhs_pv_ipv, pv_sum);

                double amt_alipay_njhs = MyUtil.parseDouble(recordValue[t + 22]);// 非聚划算支付
                double qty_alipay_njhs = MyUtil.parseDouble(recordValue[t + 23]);
                double price_alipay_njhs = MyUtil.calcPrice(amt_alipay_njhs, qty_alipay_njhs);

                double uv_ipv_ratio = MyUtil.calcRatio(pv_uv, pv_ipv);  // pv/uv
                double cart_ipv_ratio = MyUtil.calcRatio(cart_ipv, pv_ipv); // pv加购率
                double collect_uv_ratio = MyUtil.calcRatio(collect_uv, pv_uv); // uv收藏率
                double gmv_ipv_ratio = MyUtil.calcRatio(qty_gmv, pv_ipv);   // pv拍下率
                double alipay_ipv_ratio = MyUtil.calcRatio(qty_alipay, pv_ipv); // pv支付率
                double alipay_njhs_ipv_ratio = MyUtil.calcRatio(qty_alipay_njhs, pv_ipv);   // 非聚划算pv支付率
                double alipay_njhs_pay_ratio = MyUtil.calcRatio(qty_alipay_njhs, qty_alipay);// 非聚划算占总成交的比率

                double alipay_cart_ratio = MyUtil.calcRatio(qty_alipay, cart_ipv);      // 加购=>支付
                double alipay_collect_ratio = MyUtil.calcRatio(unum_alipay, collect_uv); // 收藏=>支付
                double alipay_gmv_ratio = MyUtil.calcRatio(qty_alipay, qty_gmv);        // 拍下=>支付

                double[] column = {pv_ipv, pv_uv, cart_ipv, collect_uv, qty_gmv, price_gmv,
                        qty_alipay, price_alipay,
                        ztc_pv_ratio, tbk_pv_ratio, ss_pv_ratio, jhs_pv_ratio,
                        qty_alipay_njhs, price_alipay_njhs,
                        uv_ipv_ratio, cart_ipv_ratio, collect_uv_ratio, gmv_ipv_ratio,
                        alipay_ipv_ratio, alipay_njhs_ipv_ratio, alipay_njhs_pay_ratio,
                        alipay_cart_ratio, alipay_collect_ratio, alipay_gmv_ratio};

                for (int j = 0; j < column.length; j++) {
                    result.set(++index, column[j]);
                    columns[i][j] = column[j];
                }
            }

            // 趋势特征
            for (int i = 0; i < intervals.length - 1; i++) {
                double[] columnA = columns[i];
                double[] columnB = columns[i+1];
                for (int j = 0; j < columnA.length; j++) {
                    double trend = MyUtil.calcTrend(columnA[j], columnB[j]);
                    if (j == 5 || j == 7 || j == 13) {
                        trend = MyUtil.calcPriceTrend(columnA[j], columnB[j]);
                    }
                    result.set(++index, trend);
                }
            }

            // 时间特征
            int dayIndex = myDateUtil.dayMap.get(day);
            int startDayIndex = myDateUtil.dayMap.get(startDay);
            int diffDayIndex = dayIndex - startDayIndex;
            int season = myDateUtil.getSeason(day);

            int promotionDay = 0;
            int festivalDay = 0;
            for (int i = 0; i < 14; i++) {
                Long nearDay = MyUtil.getNearDay(day, i);
                if (myDateUtil.PROMOTION_DAYS.contains(nearDay)) {
                    promotionDay = 1;
                }
                if (myDateUtil.FESTIVAL_DAYS.contains(nearDay)) {
                    festivalDay++;
                }
            }

            long minSale = Long.MAX_VALUE;
            long maxSale = Long.MIN_VALUE;
            double meanSale = 0;

            // day01~day14
            for (int i = 8; i < 22; i++) {
                long daySale = MyUtil.parseLong(recordValue[i]);
                minSale = Math.min(minSale, daySale);
                maxSale = Math.max(maxSale, daySale);
                meanSale += (double)daySale;
            }
            meanSale /= 14.0;
            double variance = 0;
            for (int i = 8; i < 22; i++) {
                long daySale = MyUtil.parseLong(recordValue[i]);
                variance += Math.pow(daySale - meanSale, 2);
            }
            double std = MyUtil.round(Math.sqrt(variance / 14.0));

            result.set(++index, diffDayIndex);
            result.set(++index, season);
            result.set(++index, promotionDay);
            result.set(++index, festivalDay);
            result.set(++index, avgSale);
            result.set(++index, std);
            result.set(++index, minSale);
            result.set(++index, maxSale);

            long level_id = result.getBigint(5);
            for (int i = 0; i < 13; i++) {
                if (i + 1 == (int)level_id) {
                    result.set(++index, 1);
                } else {
                    result.set(++index, 0);
                }
            }

            context.write(result);
        }
    }

}
