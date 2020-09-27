package com.ng.bigdata.etl

import com.ng.bigdata.util.RowUtils
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @User: kaisy
 * @Date: 2020/9/24 15:44
 * @Desc: 媒体维度统计
 */
object Terminal {
  def main(args: Array[String]): Unit = {
    // todo 1. 创建上下文
    val spark: SparkSession = SparkSession.builder().master("local").appName("terminal")
      .getOrCreate()

    // todo 2. 读取App字典文件并广播
    val dict: RDD[String] = spark.sparkContext.textFile("G:\\Learning\\classMaterial\\Third_phase\\25_Program_4_Userdmp\\mian\\app_dict.txt")
    // tip 处理App字典文件
    val dictMap: collection.Map[String, String] = dict.map(_.split("\t")).filter(_.length >= 5)
      .map((line: Array[String]) => (line(4), line(1))).collectAsMap()
    // tip 3. 广播App字典文件Map
    val broadDict: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dictMap)

    // todo 3. 读取数据源
    val logDF: DataFrame = spark.read.load("data/log/log.gz.parquet")
    // todo 数据处理 4. App
    println("----------App类----------")
    logDF.rdd.map(t => {
      val appid: String = t.getAs[String]("appid")
      var appname: String = t.getAs[String]("appname")
      // tip 判断AppName是否为空，若为空需根据appid取值
      if(StringUtils.isBlank(appname)){
        appname = broadDict.value.getOrElse(appid,"未知")
      }
      // tip 获取count 集合
      val list: List[Double] = RowUtils.countUtils(t)
      // tip 返回tuple
      (appname,list)
    })
      .reduceByKey((list1,list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })
      .foreach(t => {
        println(t._1+" : "+t._2.mkString(","))
      })

    // todo 数据处理 5. 运营
    println("----------运营类----------")
    logDF.rdd.map( t => {
      // tip 取出运营商数据
      val operator: String = t.getAs[String]("ispname")
      // tip 获取count 集合
      val list: List[Double] = RowUtils.countUtils(t)
      // tip 返回元组
      (operator,list)
    })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map( t => t._1+t._2)
      })
      .foreach(t=>println(t._1+" : "+t._2.mkString(",")))

    // todo 数据处理 6. 网络类
    println("----------网络类----------")
    logDF.rdd.map( l => {
      // tip 取出网络类别数据
      val netName: String = l.getAs[String]("networkmannername")
      // tip 获取count
      val list: List[Double] = RowUtils.countUtils(l)
      // tip 返回元组
      (netName,list)
    })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map( t => t._1+t._2)
      })
      .foreach(t=>println(t._1+" : "+t._2.mkString(",")))

    // todo 数据处理 7. 设备类
    println("----------设备类----------")
    logDF.rdd.map( g => {
      // tip 取出设备类别数据
      val deviceName: String = RowUtils.getDeviceName(g,"devicetype")
      // tip 获取count
      val list: List[Double] = RowUtils.countUtils(g)
      // tip 返回元组
      (deviceName,list)
    })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map( t => t._1+t._2)
      })
      .foreach(t=>println(t._1+" : "+t._2.mkString(",")))

    // todo 数据处理 8. 操作系
    println("----------操作系----------")
    logDF.rdd.map(x => {
      // tip 取出操作系数据
      val operatorName:String = RowUtils.getOperatorName(x,"client")
      // tip 获取count
      val list: List[Double] = RowUtils.countUtils(x)
      // tip 返回元组
      (operatorName,list)
    })
      .reduceByKey((list1,list2) => {
        list1.zip(list2).map( t => t._1+t._2)
      })
      .foreach(t=>println(t._1+" : "+t._2.mkString(",")))

   }

}
