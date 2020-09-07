package com.chudichen.spark.web.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 课程实体
 *
 * @author chudichen
 * @date 2020-09-07
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CourseClickCount {

    /** 名称 */
    private String name;
    /** 点击量 */
    private Long value;
}
