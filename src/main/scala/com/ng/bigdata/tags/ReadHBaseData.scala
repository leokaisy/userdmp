package com.ng.bigdata.tags

import org.apache.spark.sql.SparkSession

/**
 * @User: kaisy
 * @Date: 2020/9/27 22:01
 * @Desc: 读取HBase数据
 */
object ReadHBaseData {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val spark: SparkSession = SparkSession.builder().master("local").appName("readData")
      .getOrCreate()

    



  }
}
