package com.chudichen.spark.web.dao;

import com.chudichen.spark.web.domain.CourseClickCount;
import com.chudichen.spark.web.util.HBaseUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 实战课程访问量Dao层
 *
 * @author chudichen
 * @date 2020-09-07
 */
@Repository
public class CourseSearchCountDao {

    private static final String TABLE = "course_search_click";
    private static final String CF = "info";
    private static final String COLUMN = "click_count";

    public List<CourseClickCount> query(String rowKey) {
        ResultScanner scan = HBaseUtil.INSTANCE.scan(TABLE, rowKey);
        List<CourseClickCount> list = Lists.newArrayList();
        if (scan == null) {
            return list;
        }

        for (Result result : scan) {
            CourseClickCount course = new CourseClickCount();

            String key = Bytes.toString(result.getRow());
            int index = key.lastIndexOf("_");
            String name = key.substring(9,index);
            Long value = Bytes.toLong(result.getValue(Bytes.toBytes(CF), Bytes.toBytes(COLUMN)));

            course.setValue(value);
            course.setName(name);
            list.add(course);
        }
        return list;
    }
}
