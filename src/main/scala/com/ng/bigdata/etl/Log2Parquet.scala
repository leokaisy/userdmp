package com.ng.bigdata.etl

/**
 * @User: kaisy
 * @Date: 2020/9/23 14:26
 * @Desc: 数据加载，转换格式
 */
object Log2Parquet {
  def main(args: Array[String]): Unit = {
    // 创建上下文
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName)
      .getOrCreate()


  }
}
