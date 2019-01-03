package com.alex.space.spark.mains

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
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
    println("图中最大的出度、入度、度数")

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    println("Max of OutDegrees:" + graph.outDegrees.reduce(max))
    println("Max of InDegrees:" + graph.inDegrees.reduce(max))
    println("Max of Degrees:" + graph.degrees.reduce(max))



    // 转换操作
    println("*************************************************************")
    println("转换操作")
    println("*************************************************************")

    // mapVertices 对图的顶点进行转换，返回一张新图
    println("\n顶点的转换操作，顶点age+10")
    graph.mapVertices { case (id, (name, age)) => (id, (name, age + 10)) }
      .vertices
      .collect
      .foreach(v => println(s"${v._2._1} is${v._2._2}"))


    // mapEdges 对图的边进行转换，返回一张新图
    println("\n边的转换操作，边的属性*2")
    graph.mapEdges(e => e.attr * 2)
      .edges
      .collect
      .foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))


    println("*************************************************************")
    println("结构操作")
    println("*************************************************************")

    // subgraph 从图中选出一些顶点，这些顶点以及相应的边就构成了一张子图
    println("\n顶点age>25的子图")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 25)

    println("\n子图所有顶点")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    println("\n子图所有边:")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))




    // 连接操作
    println("*************************************************************")
    println("连接操作")
    println("*************************************************************")

    case class User(name: String, age: Int, inDeg: Int, outDeg: Int)

    // 创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices {
      case (_, (name, age)) => User(name, age, 0, 0)
    }

    // joinVertices 对于两个图中都存在的顶点进行转换
    // initialUserGraph与inDegrees，outDegrees(RDD)进行连接，并修改initialUserGraph中inDeg值，outDeg值
    val userGraph = initialUserGraph
      .outerJoinVertices(initialUserGraph.inDegrees) {
        case (_, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
      }
      .outerJoinVertices(initialUserGraph.outDegrees) {
        case (_, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
      }
    println("\n连接图的属性")
    userGraph.vertices.collect.foreach(v => println(s"${v._2.name} inDeg:${v._2.inDeg} outDeg:${v._2.outDeg}"))

    println("\n出度和入度相同的人员")
    userGraph.vertices
      .filter {
        case (_, v) => v.inDeg == v.outDeg
      }
      .collect
      .foreach {
        case (_, property) => println(property.name)
      }



    // 聚合操作
    println("*************************************************************")
    println("聚合操作")
    println("*************************************************************")

    // mapReduceTriplets 有一个mapFunc和一个reduceFunc
    // mapFunc对图中的每一个EdgeTriplet进行处理，生成一个或者多个消息，并且将这些消息发送个Edge的一个或者两个顶点
    // reduceFunc对发送到每一个顶点上的消息进行合并，生成最终的消息，最后返回一个VertexRDD
    println("\n找出年纪最大的追求者")
    val oldestFollower: VertexRDD[(String, Int)] = userGraph.mapReduceTriplets[(String, Int)](
      // 将源顶点的属性发送给目标顶点,map过程
      edge => Iterator((edge.dstId, (edge.srcAttr.name, edge.srcAttr.age))),
      // 得到最大追求者,reduce过程
      (a, b) => if (a._2 > b._2) a else b
    )

    userGraph.vertices
      .leftJoin(oldestFollower) { (_, user, optOldestFollower) =>
        optOldestFollower match {
          case None => s"${user.name} does not have any followers."
          case Some(oldestAge) => s"The oldest age of ${user.name}\'s followers is ${oldestAge._2}(${oldestAge._1})."
        }
      }
      .collect
      .foreach { case (_, str) => println(str) }

    // 找出追求者的平均年龄
    println("\n找出追求者的平均年龄")
    val averageAge: VertexRDD[Double] = userGraph.mapReduceTriplets[(Int, Double)](
      // 将源顶点的属性(1,Age)发送给目标顶点,map过程
      edge => Iterator((edge.dstId, (1, edge.srcAttr.age.toDouble))),
      // 得到追求者的数量和总年龄
      (a, b) => (a._1 + b._1, a._2 + b._2)
    )
      .mapValues((_, p) => p._2 / p._1)

    userGraph.vertices
      .leftJoin(averageAge) { (_, user, optAverageAge) =>
        optAverageAge match {
          case None => s"${user.name} does not have any followers."
          case Some(avgAge) => s"The average age of ${user.name} \'s followers is $avgAge."
        }
      }
      .collect
      .foreach { case (_, str) => println(str) }

    // 聚合操作
    println("*************************************************************")
    println("聚合操作")
    println("*************************************************************")

    // pregel 采用BSP模型，包括三个函数vprog、sendMsg和mergeMsg
    // vprog是运行在每个节点上的顶点更新函数，接收消息，然后对顶点属性更新
    // sendMsg生成发送给下一次迭代的消息
    // mergeMsg对同一个顶点接收到的多个消息进行合并，迭代一直进行到收敛，或者达到了设置的最大迭代次数为止
    println("\n找出3到各顶点的最短距离")
    // 定义源点
    val sourceId: VertexId = 3L
    val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (_, dist, newDist) => math.min(dist, newDist),
      // 权重计算
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      // 最短距离
      (a, b) => math.min(a, b)
    )
    println(sssp.vertices.collect.mkString("\n"))

  }
}
