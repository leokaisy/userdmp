package com.ng.bigdata.tags

import com.ng.bigdata.util.{RowUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
 * @User: kaisy
 * @Date: 2020/9/24 14:52
 * @Desc:
 */
object TagContextV2 {
  def main(args: Array[String]): Unit = {
    // todo 创建上下文
    val spark: SparkSession = SparkSession.builder().master("local").appName("tag")
      .getOrCreate()

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
    val value: RDD[(ArrayBuffer[String], Row)] = logDF.rdd.map(row => {
      (RowUtils.getAllUserId(row), row)
    })
    // todo 点集合
    val vd: RDD[(Long, ArrayBuffer[(String, Int)])] = value.flatMap(row => {
      // tip 获取用户id，拿到不为空的ID即可
      val userId = row._1.map((_, 1))
      // tip 广告位标签
      val adTags: ArrayBuffer[(String, Int)] = TagUtils.getAdTags(row._2)
      // tip appName标签
      val appTags: ArrayBuffer[(String, Int)] = TagUtils.getAppTags(row._2, broadDict)
      // tip 渠道地域标签
      val locationTags: ArrayBuffer[(String, Int)] = TagUtils.getLocationTags(row._2)
      // tip 关键字标签
      val keyWordTags: ArrayBuffer[(String, Int)] = TagUtils.getKeyWordTags(row._2)
      // tip 设备标签
      val deviceTags: ArrayBuffer[(String, Int)] = TagUtils.getDeviceTags(row._2)
      //      // tip 商圈标签
      //      val circleTags: ArrayBuffer[(String, Int)] = TagUtils.getCircleTags(row._2)
      // tip 合并标签
      val combineTags: ArrayBuffer[(String, Int)] = adTags ++ appTags ++ locationTags ++ keyWordTags ++ deviceTags
      userId.map(t => {
        if (userId.head.equals(t)) {
          (t.hashCode.toLong, combineTags)
        } else {
          (t.hashCode.toLong, ArrayBuffer[(String, Int)]())
        }
      })
    })

    // todo 构建边集合
    val ed: RDD[Edge[Int]] = value.flatMap(t => {
      t._1.map(id => Edge(t._1.head.hashCode.toLong, id.hashCode.toLong, 0))
    })

    // todo 构建图，点集合、边集合
    val graph = Graph(vd, ed)

    // todo 获取顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents.vertices

    // todo 匹配顶点属性
    vertices.join(vd).map {
      case (uid, (id, tags)) => (id, tags)
    }
      .reduceByKey((r1, r2) => {
        (r1 ++ r2)
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toBuffer
          .asInstanceOf[ArrayBuffer[(String, Int)]]
      })

    // todo 标签衰减  加载HBase数据，进行衰减0.8操作

    // todo 同事过滤掉低于0.5的标签，在和新标签聚合后即可，租后存入HBase

  }
}
