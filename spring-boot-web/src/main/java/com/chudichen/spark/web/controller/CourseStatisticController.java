package com.chudichen.spark.web.controller;


import com.chudichen.spark.web.dao.CourseClickCountDao;
import com.chudichen.spark.web.dao.CourseSearchCountDao;
import com.chudichen.spark.web.domain.CourseClickCount;
import com.google.common.collect.Maps;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.util.List;
import java.util.Map;

/**
 * 控制器
 *
 * @author chudichen
 * @date 2020-09-07
 */
@RestController
public class CourseStatisticController {

    /** 根据课程编号来获取课程名称 */
    private static final Map<String, String> MAP = Maps.newHashMap();
    /** 根据搜索引擎域名来映射搜索引擎名 */
    private static final Map<String, String> SEARCH_MAP = Maps.newHashMap();

    static {
        MAP.put("128", "10小时入门大数据");
        MAP.put("112", "大数据 Spark SQL慕课网日志分析");
        MAP.put("145", "深度学习之神经网络核心原理与算法");
        MAP.put("125", "基于Spring Boot技术栈博客系统企业级前后端实战");
        MAP.put("130", "Web前端性能优化");
        MAP.put("131", "引爆潮流技术\r\n" +
                "Vue+Django REST framework打造生鲜电商项目");

        SEARCH_MAP.put("cn.bing.com", "微软Bing");
        SEARCH_MAP.put("www.duba.com", "毒霸网址大全");
        SEARCH_MAP.put("search.yahoo.com", "雅虎");
        SEARCH_MAP.put("www.baidu.com", "百度");
        SEARCH_MAP.put("www.sogou.com", "搜狗");
    }

    private final CourseClickCountDao courseClickCountDao;
    private final CourseSearchCountDao courseSearchCountDao;

    public CourseStatisticController(CourseClickCountDao courseClickCountDao,
                                     CourseSearchCountDao courseSearchCountDao) {
        this.courseClickCountDao = courseClickCountDao;
        this.courseSearchCountDao = courseSearchCountDao;
    }

    @ResponseBody
    @PostMapping("/get_course_count")
    public List<CourseClickCount> queryCourse() {
        // 20200907_131
        return courseClickCountDao.query("20200907");
    }

    @ResponseBody
    @PostMapping("/get_course_search")
    public List<CourseClickCount> querySearch() {
        return courseSearchCountDao.query("20200907");
    }

    @GetMapping("/echarts")
    public ModelAndView echarts() {
        return new ModelAndView("course");
    }

    @GetMapping("/search")
    public ModelAndView search() {
        return new ModelAndView("search");
    }
}
