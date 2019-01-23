package com.alex.space.jobs

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * @author Alex
  *         Created by Alex on 2019/1/23.
  */
object APIDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val input: DataSet[(Int, String)] = env.fromElements(
      (1, "a"), (1, "b"), (2, "a"), (3, "c")
    )
    val output = input
      .groupBy(0)
      .reduceGroup {
        (in, out: Collector[(Int, String)]) =>

          out.collect(in.reduce((x, y) => (x._1, x._2 + y._2)))
      }

    output.print()

  }

}
