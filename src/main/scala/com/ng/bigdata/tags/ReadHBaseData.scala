package com.ng.bigdata.tags

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
 * @User: kaisy
 * @Date: 2020/9/27 22:01
 * @Desc: 读取HBase数据
 */
object ReadHBaseData {
  def main(args: Array[String]): Unit = {
    // todo 创建环境
    val spark: SparkSession = SparkSession.builder().master("local").appName("readData")
      .getOrCreate()

    // todo 读取hbase数据
    // todo 配置hbase连接
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","sparkunique:2181")
    conf.set(TableInputFormat.INPUT_TABLE,"test")

    // todo 读取HBase表
    spark.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    // tip 处理数据
      .foreach{
        case (_,result) => {
          // tip 获取rowkey
          val key: String = Bytes.toString(result.getRow)
          // tip 获取列簇列名列值
          val values: String = Bytes.toString(result.getValue(Bytes.toBytes("tags"), Bytes.toBytes("20200925")))
          // tip 衰减 将获取的字符串切分（按照存入时的拼接字符），然后获取数据字段，可以写正则匹配
          // tip 把这个代码整合到前面的标签代码中，合并标签即可
          println(s"Key: $key \t Value: $values")

       }
      }



  }
}
