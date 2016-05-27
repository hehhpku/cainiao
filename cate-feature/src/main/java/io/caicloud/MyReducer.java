package io.caicloud;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;
import com.aliyun.odps.mapred.ReducerBase;

import org.apache.commons.collections.map.HashedMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer extends ReducerBase {
    private Record result;

    public final int CART_IPV_INDEX = 9;//被加购次数
    public final int QTY_GMV_INDEX = 14;//拍下件数
    public final int QTY_ALIPAY_INDEX = 18;//成交件数
    public final int QTY_ALIPAY_NJHS_INDEX = 30;//非聚划算支付件数
    public final int COLLECT_UV_INDEX = 11;//收藏夹人次

    public final List<Integer> featureIndex = Arrays.asList(
            CART_IPV_INDEX,
            QTY_GMV_INDEX,
            QTY_ALIPAY_INDEX,
            QTY_ALIPAY_NJHS_INDEX,
            COLLECT_UV_INDEX);

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        int index = 0;
        result.set(index++, key.getBigint(0)); // brandId
        result.set(index++, key.getBigint(1)); // store_code
        result.set(index++, key.getBigint(2)); // date

        Map<Integer, Long> featureIndexValueMap = new HashMap<Integer, Long>();
        for (int idx: featureIndex){
            featureIndexValueMap.put(idx, 0L);
        }

        while (values.hasNext()) {
            Record val = values.next();
            for (int idx: featureIndex) {
                featureIndexValueMap.put(idx, featureIndexValueMap.get(idx) + val.getBigint(idx));
            }
        }

        for (int idx: featureIndex){
            result.set(index++, Double.valueOf(featureIndexValueMap.get(idx)));
        }
        context.write(result);
    }
}
