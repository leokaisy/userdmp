package com.ng.bigdata.tags

import com.ng.bigdata.util.{ RowUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/24 14:52
 * @Desc:
 */
object TagContext {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val spark: SparkSession = SparkSession.builder().master("local").appName("tag")
      .getOrCreate()

    // todo HBase参数配置
    val conf: Configuration = spark.sparkContext.hadoopConfiguration
    // todo 配置链接
    conf.set("hbase.zookeeper.quorum","sparkunique:2181")
    // todo 创建连接
    val hbCon: Connection = ConnectionFactory.createConnection(conf)
    // tip 获取管理对象
    val hbAdmin: Admin = hbCon.getAdmin
    if(!hbAdmin.tableExists(TableName.valueOf("test"))){
      println("此表不存在，需要创建...")
      // tip 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf("test"))
      // tip 设计列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // tip 将列簇加入表中
      tableDescriptor.addFamily(columnDescriptor)
      hbAdmin.createTable(tableDescriptor)
      // todo 关闭连接，表
      hbAdmin.close()
      hbCon.close()
    }
    // todo 创建Hadoop任务
    val jobConf = new JobConf(conf)
    // todo 执行任务属性
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // todo 执行写入至哪个表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"test")


    // todo 加载字典文件
    val dict: RDD[String] = spark.sparkContext.textFile("G:\\Learning\\classMaterial\\Third_phase\\25_Program_4_Userdmp\\mian\\app_dict.txt")
    // tip 处理App字典文件
    val dictMap: collection.Map[String, String] = dict.map(_.split("\t")).filter(_.length >= 5)
      .map((line: Array[String]) => (line(4), line(1))).collectAsMap()
    // tip 广播App字典文件Map
    val broadDict: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dictMap)

    // todo 读取数据文件
    val logDF: DataFrame = spark.read.load("data/log/log.gz.parquet")

    // todo 数据处理
    logDF.rdd.map((row: Row) => {
      // tip 获取用户id，拿到不为空的ID即可
      val userid: String = RowUtils.getUserID(row)
      // tip 广告位标签
      val adTags: ArrayBuffer[(String, Int)] = TagUtils.getAdTags(row)
      // tip appName标签
      val appTags: ArrayBuffer[(String, Int)] = TagUtils.getAppTags(row,broadDict)
      // tip 渠道地域标签
      val locationTags: ArrayBuffer[(String, Int)] = TagUtils.getLocationTags(row)
      // tip 关键字标签
      val keyWordTags: ArrayBuffer[(String, Int)] = TagUtils.getKeyWordTags(row)
      // tip 设备标签
      val deviceTags: ArrayBuffer[(String, Int)] = TagUtils.getDeviceTags(row)
      // tip 商圈标签
      val circleTags: ArrayBuffer[(String, Int)] = TagUtils.getCircleTags(row)

      // tip 合并标签
      val combineTags: ArrayBuffer[(String, Int)] = adTags ++ appTags ++ locationTags ++ keyWordTags ++ deviceTags
//      ++ circleTags
//      val combineTags: ArrayBuffer[(String, Int)] =  circleTags

      // tip 返回元组
      (userid, combineTags)
    })
      .reduceByKey((arr1 , arr2) => {
        (arr1 ++ arr2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toBuffer
          .asInstanceOf[ArrayBuffer[(String,Int)]]
      })
//      .foreach(println)
      .map{
        case (userId,userTags) => {
          // 创建rowkey
          val put = new Put(Bytes.toBytes(userId))
          // 列值
          put.addImmutable(Bytes.toBytes("tags"),// tip 列簇
            Bytes.toBytes("20200925"), // tip 列名
            Bytes.toBytes(userTags.mkString("-")))  // tip 列值

          // todo 返回结果
          (new ImmutableBytesWritable(),put)
        }
      }
      .saveAsHadoopDataset(jobConf)
  }
}
