package com.ng.bigdata.etl

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * @User: kaisy
 * @Date: 2020/9/24 9:47
 * @Desc:
 */
object ProCityCnt {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val spark: SparkSession = SparkSession.builder().master("local").appName("pc_count")
      .getOrCreate()

    // todo 导入隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._

    // todo 读取文件数据
    val logDF: DataFrame = spark.read.parquet("data/log/log.gz.parquet")

    // todo 配置mysql数据库的配置
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "201212")

    // todo 统计各个省份的数据量分布情况
    val value: Dataset[Row] = logDF.groupBy('provincename, 'cityname)
      .agg(countDistinct('sessionid).as("ct"))
      .orderBy(desc("ct"))
    // todo 将结果写入mysql
//    value.write
//      .mode(SaveMode.Append)
//      .jdbc("jdbc:mysql://localhost:3306/test", "logCityCount", prop)

    // todo 将结果转成json格式文件存入hdfs
    value.coalesce(1).write.partitionBy("provincename", "cityname").json("data/count_log")
  }
}
