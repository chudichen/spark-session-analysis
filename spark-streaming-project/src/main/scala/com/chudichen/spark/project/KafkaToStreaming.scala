package com.chudichen.spark.project

import com.chudichen.spark.BaseSpark
import com.chudichen.spark.dao.{CourseClickCountDao, CourseSearchCountDao}
import com.chudichen.spark.domain.{CheckLog, CourseClickCount, CourseSearchCount}
import com.chudichen.spark.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * Kafka消费
 *
 * @author chudichen
 * @date 2020-08-31
 */
object KafkaToStreaming extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("kafkaToStreaming")
      .setMaster("local[2]")

    // 一分钟执行一次
    val ssc = new StreamingContext(conf, Seconds(5))
    // 然后创建一个set,里面放入你要读取的Topic,这个就是我们所说的,它给你做的很好,可以并行读取多个topic
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "127.0.0.1:9092")
    val topics = Set[String]("Streaming");
    //kafka返回的数据时key/value形式，后面只要对value进行分割就ok了
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)

    val logInfos = stream.map(_._2)
      .map(convertToCheckLog)
      // 过滤掉无效数据
      .filter(checkData)

    // 从今天到现在为止实战课程的访问量写入数据库
    logInfos.map(x => {
      // 将CheckLog格式的数据转为20180724_courseId
      (x.time.substring(0, 8) + "_" + x.courseId, 1)
    }).reduceByKey(_+_)
      .foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list = new ListBuffer[CourseClickCount]
        partition.foreach(info => {
          list.append(CourseClickCount(info._1, info._2))
        })
        // 将数据保存到HBase
        CourseClickCountDao.increase(list)
      })
    })

    // 统计从搜索引擎引流过来的访问了，最后写入数据库
    logInfos.map(logInfo => {
      val url = logInfo.referes.replaceAll("//", "/")
      val splits = url.split("/")
      val search = if (splits.length >= 2) splits(1) else ""
      (logInfo.time.substring(0, 8), logInfo.courseId, search)
    }).filter(_._3 != "")
      .map(info => {
        val day_search_course = info._1 + "_" + info._3 + "_" + info._2
        (day_search_course, 1)
      })
      .reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val list=new ListBuffer[CourseSearchCount]
          partition.foreach(info=>{
            list.append(CourseSearchCount(info._1,info._2))
          })
          CourseSearchCountDao.increase(list)      //保存到HBase数据库
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 过滤掉无效订单
   *
   * @param checkLog 日志
   * @return {@code true}标示有效
   */
  def checkData(checkLog: CheckLog): Boolean = {
    checkLog.courseId != 0
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
