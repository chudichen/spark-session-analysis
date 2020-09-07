package com.chudichen.spark.demo

import com.chudichen.spark.BaseSpark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 带状态的word count
 *
 * @author chudichen
 * @date 2020-08-31
 */
object StatefulWordCount extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StatefulWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))

    // 使用stateful算子必须设置checkPoint
    // 在生产环境中建议将checkPoint设置为hdfs文件系统中的一个文件
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("localhost", 12345)
    val results = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)
    val state = results.updateStateByKey[Int](updateFunc)
    state.print()

    // 启动spark streaming
    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }
}
