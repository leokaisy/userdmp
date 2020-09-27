package com.ng.bigdata.etl

import com.ng.bigdata.util.RowUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @User: kaisy
 * @Date: 2020/9/24 10:52
 * @Desc: 地域维度指标统计
 */
object RequestProcessCounts {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("pc_count")
      .getOrCreate()

    // todo 导入隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // todo 读取数据
    val logDF: DataFrame = spark.read.parquet("data/log/log.gz.parquet")

    // todo rdd处理数据
    logDF.rdd.map(row => {
      // 方法判断
      val tuple = RowUtils.countUtils(row)

      // 返回tuple
      ((row.getAs[String]("provincename"),row.getAs[String]("cityname")),tuple)
    })
      .reduceByKey((list1,list2) =>{
        list1.zip(list2).map(t => t._1 + t._2)
      })
      .foreach(t => {
        println(t._1+" : "+t._2.mkString(","))
      })
  }
}
