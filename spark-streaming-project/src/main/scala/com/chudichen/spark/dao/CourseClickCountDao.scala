package com.chudichen.spark.dao

import com.chudichen.spark.domain.CourseClickCount
import com.chudichen.spark.utils.HBaseUtil

import scala.collection.mutable.ListBuffer

/**
 * 访问数据库DAO层
 * 今天到现在为止的实战课程的访问量
 *
 * @author chudichen
 * @date 2020-09-01
 */
object CourseClickCountDao {

  val table = "course_click_count"
  val cf = "info"
  val columnQualifier = "click_count"

  /**
   * 像数据库中保存数据
   *
   * @param courseList 课程
   */
  def save(courseList: ListBuffer[CourseClickCount]): Unit = {
    courseList.foreach(course => {
      val rowKey = course.day_course
      val value = course.clickCount
      HBaseUtil.INSTANCE.putData(table, rowKey, cf, columnQualifier, value)
    })
  }

  /**
   * 与旧值进行累加
   *
   * @param courseList 课程列表
   */
  def increase(courseList: ListBuffer[CourseClickCount]): Unit = {
    courseList.foreach(course => {
      val rowKey = course.day_course
      val value = course.clickCount
      val oldValue = HBaseUtil.INSTANCE.getDataLong(table, rowKey, cf, columnQualifier)
      val newValue = oldValue + value
      HBaseUtil.INSTANCE.putData(table, rowKey, cf, columnQualifier, newValue)
    })
  }

  /**
   * 从数据库
   *
   * @param day_course 课程
   * @return
   */
  def get(day_course: String): Long = {
    HBaseUtil.INSTANCE.getDataLong(table, day_course, cf, columnQualifier)
  }
}
