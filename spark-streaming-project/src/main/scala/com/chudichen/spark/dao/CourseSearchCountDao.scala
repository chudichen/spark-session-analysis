package com.chudichen.spark.dao

import com.chudichen.spark.domain.CourseSearchCount
import com.chudichen.spark.utils.HBaseUtil

import scala.collection.Iterator
import scala.collection.mutable.ListBuffer

/**
 * 实战课程访问量DAO层
 *
 * @author chudichen
 * @date 2020-09-02
 */
object CourseSearchCountDao {

  val table = "course_search_click"
  val cf = "info"
  val columnQualifier = "click_count"

  /**
   * 向数据库中保存数据
   *
   * @param courseSearchList 课程列表
   */
  def save(courseSearchList: ListBuffer[CourseSearchCount]): Unit = {
    courseSearchList.foreach(course => {
      val rowKey = course.daySearchCourse
      val value = course.clickCount
      HBaseUtil.INSTANCE.putData(table, rowKey, cf, columnQualifier, value)
    })
  }

  def get(daySearchCourse: String): Long = {
    HBaseUtil.INSTANCE.getDataLong(table, daySearchCourse, cf, columnQualifier)
  }

  def increase(list: ListBuffer[CourseSearchCount]): Unit = {
    list.foreach(courseSearchCount => {
      val count = get(courseSearchCount.daySearchCourse) + courseSearchCount.clickCount
      HBaseUtil.INSTANCE.putData(table, courseSearchCount.daySearchCourse, cf, columnQualifier, count)
    })
  }
}
