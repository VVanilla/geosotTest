package com.iwhere

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object FirstSparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile("/user/root/test/words.txt")

    line
      .flatMap(_.split(","))
      .map(x => {
        (x.trim, 1)
      })
      .reduceByKey((a, b) => a + b)
      .saveAsTextFile("/user/root/test/words_res.txt")

  }
}
