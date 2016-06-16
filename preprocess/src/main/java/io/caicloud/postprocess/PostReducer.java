package io.caicloud.postprocess;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import io.caicloud.util.MyUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class PostReducer implements Reducer {
    private Record result;
    private final long END_DAY = 20151227;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        // 十二月的销量及预测
        double eval_sale = 0;
        double eval_prediction = 0;

        double less = 0;
        double more = 0;

        boolean isValid = false;    // 是否含有预测日期
        Record val = null;
        Map<Long, Object[]> map = new HashMap<Long, Object[]>();
        while (values.hasNext()) {
            val = values.next();

            long thedate = val.getBigint(0);
            double sale = val.getDouble(3);
            double prediction = val.getDouble(4);
            less = val.getDouble(5);
            more = val.getDouble(6);

            if (thedate < 20151101) {
                continue;
            }

            if (thedate < END_DAY) {
                map.put(thedate, val.toArray());
            } else if (thedate == END_DAY) {
                eval_sale = sale;
                eval_prediction = prediction;
                isValid = true;
            } else {
                // pass
            }
        }

        if (!isValid) {
            return;
        }

        double bestLoss = Double.MAX_VALUE;
        double bestRate = 1.0;
        int low = 0;
        int high = 0;

        if (less < more) {
            if (eval_prediction < 50) {
                low = 80;
                high = 105;
            } else if (eval_prediction < 1000) {
                low = 85;
                high = 105;
            } else {
                low = 90;
                high = 105;
            }
        } else {
            if (eval_prediction < 50) {
                low = 95;
                high = 120;
            } else if (eval_prediction < 1000) {
                low = 95;
                high = 125;
            } else {
                low = 95;
                high = 130;
            }
        }


        for (int r = low; r <= high; r++) {
            double rate = r / 100.0;

            double loss = 0;
            for (long day : map.keySet()) {
                Object[] record = map.get(day);
                double sale = MyUtil.parseDouble(record[3]);
                double prediction = MyUtil.parseDouble(record[4]);
                loss += MyUtil.getLoss(prediction * rate, sale, less, more);
            }
            if (loss < bestLoss) {
                bestLoss = loss;
                bestRate = rate;
            }
        }

        int index = -1;
//        for (int i = 0; i < 3; i++) {
//            result.set(++index, val.get(i));
//        }
        result.set(++index, END_DAY);
        result.set(++index, val.get(1));
        result.set(++index, val.get(2));
        result.set(++index, eval_sale);
        result.set(++index, eval_prediction);
        result.set(++index, eval_prediction * bestRate);
        result.set(++index, bestRate);

        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
}
