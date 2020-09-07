package com.chudichen.spark.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 日期工具类
 *
 * @author chudichen
 * @date 2020-09-01
 */
object DateUtils {

  val OLD_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val TARGET_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def getTime(time: String): Long = {
    OLD_FORMAT.parse(time).getTime
  }

  def parseTime(time: String): String = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

}
