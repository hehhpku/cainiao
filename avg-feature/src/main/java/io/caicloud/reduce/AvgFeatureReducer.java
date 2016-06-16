package io.caicloud.reduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.ReducerBase;
import io.caicloud.util.MyDateUtil;
import io.caicloud.util.MyUtil;

import java.io.IOException;
import java.util.*;

/**
 * AvgFeatureReducer.java
 *
 * @author hehuihui@meituan.com
 * @date 2016-05-20
 * @brief
 */


public class AvgFeatureReducer extends ReducerBase {
    private Record result;
    private final List<Integer> intervals = Arrays.asList(1, 3, 7, 14, 28, 56);
    private MyDateUtil myDateUtil = new MyDateUtil();

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    @Override
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        Map<Long, Object[]> map = new HashMap<Long, Object[]>();
        Long startDay = myDateUtil.END_DAY;
        while (values.hasNext()) {
            Record val = values.next();
            Long day = val.getBigint(0);
            map.put(day, val.toArray());
            if (startDay > day) {
                startDay = day;
            }
        }

        for (Map.Entry<Long, Object[]> entry : map.entrySet()) {
            Long day = entry.getKey();
            Object[] recordValue = entry.getValue();

            // 计算当前日期后14天的总销量（不包含当天）
            long saleSum = 0L;
            for (int i = 1; i <= 14; i++) {
                Long nearDay = MyUtil.getNearDay(day, i);
                Object[] nearRecord = map.get(nearDay);
                if (nearRecord != null) {
                    saleSum += Long.parseLong(nearRecord[30].toString());
                }
            }

            int index = 0;

            // 设置label：后14天的销量之和
            result.set(index, saleSum);

            // 设置key: date/item/store_code/cate_id/level_id/brand_id/supplier_id
            for (int i = 0; i < 7; i++) {
                result.set(++index, Long.parseLong(recordValue[i].toString()));
            }

            // 获取前14天的每一天销量（包含当天）
            for (int i = 0; i < 14; i++) {
                Long nearDay = MyUtil.getNearDay(day, -i);
                Object[] nearRecord = map.get(nearDay);
                Long sale = 0L;
                if (nearRecord != null) {
                    sale = Long.parseLong(nearRecord[30].toString());
                }
                result.set(++index, sale);
            }

            // 获取各个时间段前N天的feature（包含当天）
            for (int interval : intervals) {
                double[] sumRecord = new double[32];
                Arrays.fill(sumRecord, 0d);
                int N = 0;
                for (int i = 0; i < interval; i++) {
                    Long nearDay = MyUtil.getNearDay(day, -i);
                    Object[] nearRecord = map.get(nearDay);
                    if (nearRecord != null) {
                        for (int j = 7; j < 32; j++) {
                            sumRecord[j] += MyUtil.parseDouble(nearRecord[j]);
                        }
                    }
                    if (nearDay < startDay) {
                        break;
                    }
                    N++;
                }

                for (int j = 7; j < 32; j++) {
                    double avg = MyUtil.round(sumRecord[j] / Math.max(N, 1));
                    result.set(++index, avg);
                }
            }

            context.write(result);
        }
    }

}
