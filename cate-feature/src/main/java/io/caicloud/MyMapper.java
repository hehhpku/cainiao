package io.caicloud;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;
import com.aliyun.odps.mapred.MapperBase;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper extends MapperBase {
    private Record keyRecord;
    private Record valueRecord;

    public final int BRAND_INDEX = 5;
    public final int STORECODE_INDEX = 2;
    public final int THE_DATE_INDEX = 0;

    public void setup(TaskContext context) throws IOException {
        keyRecord = context.createMapOutputKeyRecord();
        valueRecord = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
        //prepare key
        long brandId = record.getBigint(BRAND_INDEX);
        long storeCode = record.getBigint(STORECODE_INDEX);
        long thedate = record.getBigint(THE_DATE_INDEX);
        keyRecord.set(0,brandId);
        keyRecord.set(1,storeCode);
        keyRecord.set(2, thedate);

        //prepare value
//        valueRecord.set(record.toArray());
        for (int i = 0; i < record.getColumnCount(); i++) {
            valueRecord.set(i, record.get(i));
        }
        context.write(keyRecord, valueRecord);
    }

}