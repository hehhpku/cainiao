package io.caicloud.util;

import com.aliyun.odps.data.Record;

/**
 * MyRecord.java
 *
 * @author hehuihui@meituan.com
 * @date 2016-05-23
 * @brief
 */

public class MyRecord  {
    private Object[] objects;

    public MyRecord() {
    }

    public MyRecord(Object[] record) {
        for (int i = 0; i < record.length; i++) {
            System.out.println("length =" + record.length + "i =" + i + "; value =" + record[i].toString());
            objects[i] = record[i];
        }
    }

    public void init(Record record) {
        for (int i = 0; i < record.getColumnCount(); i++) {
            System.out.println("i =" + i + "; value =" + record.get(i).toString());
            objects[i] = record.get(i);
        }
    }

    public Object get(int i) {
        return objects[i];
    }

    public String getString(int i) {
        return objects[i].toString();
    }

    public Long getBigint(int i) {
        return Long.parseLong(this.getString(i));
    }

    public Double getDouble(int i) {
        return Double.parseDouble(this.getString(i));
    }

}
