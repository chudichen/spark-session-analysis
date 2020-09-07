package com.chudichen.spark.dao

import com.chudichen.spark.domain.CourseClickCount
import org.junit.jupiter.api.Test

import scala.collection.mutable.ListBuffer

/**
 * @author chudichen
 * @date 2020-09-02
 */
class CourseClickDaoTest {

  @Test
  def putDataTest(): Unit = {
    val list = new ListBuffer[CourseClickCount]
    list.append(CourseClickCount("20180724_45", 85))
    list.append(CourseClickCount("20180724_124", 45))
    list.append(CourseClickCount("20180724_189", 15))
    CourseClickCountDao.save(list)
  }

  @Test
  def getDataTest(): Unit = {
    val result = CourseClickCountDao.get("20200901_145")
    println(result)
  }

}
