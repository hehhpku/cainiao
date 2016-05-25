package io.caicloud;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;
    private TableInfo[] outputTableInfo;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
        outputTableInfo = context.getOutputTableInfo();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        long count = 0;
        while (values.hasNext()) {
            Record val = values.next();
            count += val.getBigint(0);
        }
        result.set(0, key.get(0));
        result.set(1, count);
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {
        for (TableInfo tableInfo : outputTableInfo) {
//            Map<String, Integer> columnIdMap = MyUtil.getColumnIdMap(tableInfo);
//            System.out.println(columnIdMap);
            System.out.println(tableInfo.getProjectName());
            System.out.println(tableInfo.getCols());
            System.out.println(tableInfo.getLabel());
            System.out.println(tableInfo.getPartPath());
            System.out.println(tableInfo.getPartSpec());
            System.out.println(tableInfo.getTableName());
        }
    }
}
