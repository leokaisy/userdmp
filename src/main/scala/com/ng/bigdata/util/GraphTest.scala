package com.ng.bigdata.util

import org.apache.spark.sql.SparkSession

/**
 * @User: kaisy
 * @Date: 2020/9/27 14:06
 * @Desc:
 */
object GraphTest {
  def main(args: Array[String]): Unit = {

    // todo 创建上下文
    val spark: SparkSession = SparkSession.builder().appName("graph").master("local")
      .getOrCreate()
    // todo
//    spark.sparkContext.makeRDD(Seq(
//      (1L,("James",36)),
//
//
//    ))




  }
}
