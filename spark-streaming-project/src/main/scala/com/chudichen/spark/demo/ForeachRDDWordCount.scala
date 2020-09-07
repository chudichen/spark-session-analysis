package com.chudichen.spark.demo

import java.sql.{Connection, DriverManager}

import com.chudichen.spark.BaseSpark
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author chudichen
 * @date 2020-08-31
 */
object ForeachRDDWordCount extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 12345)

    val results = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

    results.print()

    results.foreachRDD(rdd => {
      rdd.foreachPartition(partitions => {
        val connection = createConnection()
        partitions.foreach(records => {
          val select = "select word from wordcount where word = '" + records._1 + "'"
          val wordIsExist = connection.createStatement().executeQuery(select)
          // 如果数据库中已经存在该数据，更新count
          if (wordIsExist.next()) {
            val updateCount = "update wordcount set count=(" + records._2 + " + count) where word = '" + records._1 +"'"
            connection.createStatement().executeUpdate(updateCount)
          } else {
            // 如果不存在直接插入统计结果
            val insertWordCount = "insert into wordcount(word, count) values ('" + records._1 + "', '" + records._2 + "')"
            connection.createStatement().execute(insertWordCount)
          }
        })
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection(): Connection = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/spark_streaming","root","root")
  }
}
