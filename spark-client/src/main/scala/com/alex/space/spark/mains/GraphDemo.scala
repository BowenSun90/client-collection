package com.alex.space.spark.mains

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * @author Alex
  *         Created by Alex on 2019/1/3.
  */
object GraphDemo extends BaseWorker {

  def main(args: Array[String]): Unit = {
    // 设置运行环境
    val conf = sparkConf
    val sc = new SparkContext(conf)

    // 顶点
    // (id, (name, age))
    val vertexArray: Array[(Long, (String, Int))] = Array(
      (1L, ("Alice", 38)),
      (2L, ("Henry", 27)),
      (3L, ("Charlie", 55)),
      (4L, ("Peter", 32)),
      (5L, ("Mike", 35)),
      (6L, ("Kate", 23))
    )

    // 边
    val edgeArray: Array[Edge[Int]] = Array(
      Edge(2L, 1L, 5),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 7),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 3),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 8)
    )

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    // 构造图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // 图的属性操作
    println("*************************************************************")
    println("属性演示")
    println("*************************************************************")

    // vertices 图的所有顶点
    println("\n图中年龄大于20的 vertices")
    graph.vertices
      .filter { case (_, (_, age)) => age > 20 }
      .collect
      .foreach { case (_, (name, age)) => println(s"$name is $age") }


    // edges 图中所有的边
    println("\n图中属性大于3的 edges")
    graph.edges
      .filter(e => e.attr > 3)
      .collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))


    // triplets 源顶点，目的顶点，以及两个顶点之间的边
    println("\n图中所有的 triples")
    for (triplet <- graph.triplets.collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    println("\n图中边属性 >3 的 triples")
    for (triplet <- graph.triplets.filter(t => t.attr > 3).collect) {
      println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
    }

    // degree 图中所有顶点的度\入度（inDegrees）\出度（outDegrees）
    println("图中最大的出度,入度,度数")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("Max of OutDegrees:" + graph.outDegrees.reduce(max))
    println("Max of InDegrees:" + graph.inDegrees.reduce(max))
    println("Max of Degrees:" + graph.degrees.reduce(max))
    println

  }
}
