package com.chudichen.spark.demo

import com.chudichen.spark.BaseSpark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * transform算子案例：黑名单的过滤
 *
 * @author chudichen
 * @date 2020-08-31
 */
object TransformApp extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TransformApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 12345)

    /*
     * 构建黑名单
     * 实际中存在数据库中
     */
    val blacks = List("jack", "leo")
    // 转换为RDD jack=>(jack, true)
    val blacksRDD = ssc.sparkContext.parallelize(blacks).map(x => (x, true))
    val checkLog = lines.map(x => (x.split(" ")(1),x)).transform(rdd => {
      rdd.leftOuterJoin(blacksRDD)
        .filter(x => !x._2._2.getOrElse(false))
        .map(name => name._2._1)
    })

    checkLog.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
