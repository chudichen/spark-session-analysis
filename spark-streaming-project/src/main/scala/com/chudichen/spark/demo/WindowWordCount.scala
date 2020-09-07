package com.chudichen.spark.demo

import com.chudichen.spark.BaseSpark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * window窗口函数的使用
 *
 * @author chudichen
 * @date 2020-08-31
 */
object WindowWordCount extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WindowWordCount")
      .setMaster("local[2]")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 12345)

    val results = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(30), Seconds(10))

    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
