package com.wadejensen.sparkjson.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  val conf: SparkConf = new SparkConf()
    .setAppName("Test Spark")
    .setMaster("local[16]")
    .set("spark.sql.shuffle.partitions", "4")
  lazy val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate
}
