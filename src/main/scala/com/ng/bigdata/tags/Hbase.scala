package com.ng.bigdata.tags

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.spark.sql.SparkSession


/**
 * @User: kaisy
 * @Date: 2020/9/25 14:57
 * @Desc:
 */
object Hbase {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("hbase_test").master("local").getOrCreate()
    val conf = new Configuration()
    conf.set("hbase.zookeeper.quorum","sparkunique:2181")
    val conn: Connection = ConnectionFactory.createConnection(conf)
    val admin: Admin = conn.getAdmin
    val descriptors: Array[NamespaceDescriptor] = admin.listNamespaceDescriptors()
    descriptors.foreach(println)

  }
}
