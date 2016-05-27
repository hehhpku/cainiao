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
    private final int[] intervals = {3, 5, 7, 10, 14, 28};
    private MyDateUtil myDateUtil = new MyDateUtil();

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Long starDay = myDateUtil.END_DAY;
        Map<Long, Object[]> map = new HashMap<Long, Object[]>();
        while (values.hasNext()) {
            Record val = values.next();
            Long day = val.getBigint(1);
            if (starDay > day) {
                starDay = day;
            }
            map.put(day, val.toArray());
        }

        for (Map.Entry<Long, Object[]> entry : map.entrySet()) {
            Long day = entry.getKey();
            if (myDateUtil.getDiffDays(day, starDay) < 14 && starDay < myDateUtil.TRAIN_END_DAY) {
                continue;
            }
            Object[] recordValue = entry.getValue();

            int index = -1;

            // sale_sum,thedate,item_id,store_code,cate,level,brand,supplier,day01~day14
            for (int i = 0; i < 22; i++) {
                result.set(++index, recordValue[i]);
            }

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
                    double trend = MyUtil.round(columnA[j] - columnB[j]);
                    result.set(++index, trend);
                }
            }

            context.write(result);
        }
    }

}
