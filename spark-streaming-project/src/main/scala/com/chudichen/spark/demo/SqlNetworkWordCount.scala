package com.chudichen.spark.demo

import com.chudichen.spark.BaseSpark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Spark Streaming整合Spark SQL
 *
 * @author chudichen
 * @date 2020-08-31
 */
object SqlNetworkWordCount extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SqlNetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 12345)
    val words = lines.flatMap(_.split(" "))
    // RDD => DataFrame
    words.foreachRDD { (rdd: RDD[String], time:Time) =>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)

      import spark.implicits._
      val wordsDataFrame = rdd.map(w => Record(w)).toDF()

      wordsDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      println(s"===========time==============")
      wordCountsDataFrame.show()
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /** 为了将RDD转化为DataFrame */
  case class Record(word: String)

  /**
   * 懒加载
   */
  object SparkSessionSingleton {
    @transient private var instance: SparkSession = _
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
