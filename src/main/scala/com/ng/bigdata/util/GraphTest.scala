package com.ng.bigdata.util

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession

/**
 * @User: kaisy
 * @Date: 2020/9/27 14:06
 * @Desc:
 */
object GraphTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()
    // todo 点集合
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L, ("老詹", 36)),
      (2L, ("浓眉", 27)),
      (6L, ("张三", 36)),
      (9L, ("李四", 36)),
      (133L, ("王五", 36)),
      (138L, ("约老师", 23)),
      (16L, ("穆雷", 22)),
      (21L, ("赵六", 36)),
      (44L, ("田七", 36)),
      (158L, ("C罗", 35)),
      (5L, ("梅西", 32)),
      (7L, ("苏亚雷斯", 30))
    ))
    // todo 边集合（两个点相连就是一个边）
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    // todo 构建图
    /**
     * vertices: RDD[(VertexId, VD)],
     * edges: RDD[Edge[ED]],
     */
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    // todo 获取图的顶点ID
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    vertices.join(vertexRDD)
      .map{
        case(userId,(id,(name,age)))=>(id,List((name,age)))
      }
      .reduceByKey(_++_).foreach(println)
  }
}
