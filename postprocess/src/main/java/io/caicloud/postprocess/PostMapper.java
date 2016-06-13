package io.caicloud.postprocess;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

public class PostMapper extends MapperBase {
    Record keyRecord;
    Record valueRecord;

    @Override
    public void setup(TaskContext context) throws IOException {
        keyRecord = context.createMapOutputKeyRecord();
        valueRecord = context.createMapOutputValueRecord();
    }

    @Override
    public void map(long key, Record record, TaskContext context) throws IOException {
        keyRecord.set(0, record.getBigint(1));  // item_id
        keyRecord.set(1, record.getBigint(2));  // store_code

        for (int i = 0; i < record.getColumnCount(); i++) {
            valueRecord.set(i, record.get(i));
        }

        context.write(keyRecord, valueRecord);
    }
}
