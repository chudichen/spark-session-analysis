package com.chudichen.spark

import com.chudichen.spark.domain.CheckLog
import com.chudichen.spark.utils.DateUtils

/**
 * @author chudichen
 * @date 2020-09-01
 */
object Test {

  def main(args: Array[String]): Unit = {
    val data = "168.59.187.164\t2020-09-01 16:10:17\t\"GET /class/130.html HTTP/1.1\"\t200\thttps://www.baidu.com/s?wd=Storm实战"
    val log = convertToCheckLog(data)
    println(log)
  }


  def convertToCheckLog(line: String): CheckLog = {
    try {
      val infos = line.split("\t")
      val ip = infos(0)
      val time = DateUtils.parseTime(infos(1))
      var courseId = 0
      val url = infos(2).split(" ")(1)
      if (url.startsWith("/class")) {
        val courceHtml = url.split("/")(2)
        courseId = courceHtml.substring(0, courceHtml.lastIndexOf(".")).toInt
      }
      CheckLog(ip, time, courseId, infos(3).toInt, infos(4))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        CheckLog(null, null, 0, 0, null)
    }
  }
}
