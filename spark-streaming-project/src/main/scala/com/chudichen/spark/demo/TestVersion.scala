package com.chudichen.spark.demo

/**
 * @author chudichen
 * @date 2020-08-31
 */
object TestVersion {

  def main(args: Array[String]): Unit = {
    val version = scala.util.Properties.releaseVersion
    println(version)
  }
}
