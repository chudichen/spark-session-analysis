package com.chudichen.spark.web.dao;

import com.chudichen.spark.web.domain.CourseClickCount;
import com.chudichen.spark.web.util.HBaseUtil;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;

/**
 * 实战课程访问了Dao
 *
 * @author chudichen
 * @date 2020-09-07
 */
@Repository
public class CourseClickCountDao {

    private static final String CF = "info";
    private static final String COLUMN = "click_count";
    private static final String TABLE = "course_click_count";


    public List<CourseClickCount> query(String dayCourse) {
        ResultScanner scan = HBaseUtil.INSTANCE.scan(TABLE, dayCourse);
        if (scan == null) {
            return Collections.emptyList();
        }

        List<CourseClickCount> courseClickCounts = Lists.newArrayList();
        for (Result result : scan) {
            courseClickCounts.add(new CourseClickCount(Bytes.toString(result.getRow()),
                    Bytes.toLong(result.getValue(Bytes.toBytes(CF), Bytes.toBytes(COLUMN)))));

        }
        return courseClickCounts;
    }
}
