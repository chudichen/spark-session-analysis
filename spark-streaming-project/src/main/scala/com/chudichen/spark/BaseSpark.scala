package com.chudichen.spark

import org.apache.log4j.{Level, Logger}

trait BaseSpark {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
}