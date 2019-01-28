package com.alex.space.jobs

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * @author Alex
  *         Created by Alex on 2019/1/23.
  */
object APIDemo {

  def main(args: Array[String]): Unit = {

    //    reduceGroup()

    //    aggregate()

    //    coGroup()

    //    pi()

    copy()
  }

  def reduceGroup(): Unit = {
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

  def aggregate(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(Int, String, Double)] = env.fromElements(
      (1, "b", 20d), (1, "a", 10d), (2, "a", 30d)
    )
    input.distinct(0, 1)

    val output: DataSet[(Int, String, Double)] = input
      .groupBy(1)
      // select tuple with minimum values for first and third field.
      .minBy(0, 2)

    output.print()
  }

  def coGroup(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val iVals: DataSet[(String, Int)] = env.fromElements(("a", 10), ("b", 20), ("a", 30))
    val dVals: DataSet[(String, Double)] = env.fromElements(("a", 1.0), ("b", 2.0), ("c", 3.0))

    val output: DataSet[Double] = iVals.coGroup(dVals).where(0).equalTo(0) {
      (iVals, dVals, out: Collector[Double]) =>
        val ints = iVals map {
          _._2
        } toSet

        for (dVal <- dVals) {
          for (i <- ints) {
            out.collect(dVal._2 * i)
          }
        }
    }

    output.print()
  }

  def pi(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val initial = env.fromElements(0)

    val count = initial.iterate(10000) { iterationInput: DataSet[Int] =>
      val result = iterationInput.map { i =>
        val x = Math.random()
        val y = Math.random()
        i + (if (x * x + y * y < 1) 1 else 0)
      }
      result
    }

    val result = count map { c => c / 10000.0 * 4 }

    env.execute("Iterative Pi Example")

    result.print()
  }

  def copy(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val input: DataSet[(Int, String)] = env.fromElements(
      (1, "a"), (1, "b"), (2, "a"), (3, "c")
    )

    val ds = input.map(x => {
      println("map1:" + x)
      (x._2, x._1)
    })
      .map(x => {
        println("map2:" + x)
        (x._1, x._2 + 1)
      })

    ds.output(new OutputFormat[(String, Int)] {
      override def configure(configuration: Configuration): Unit = {}

      override def writeRecord(it: (String, Int)): Unit = {
        println("output1: " + it._1 + " " + it._2)
      }

      override def close(): Unit = {}

      override def open(i: Int, i1: Int): Unit = {}

    })

    ds.groupBy(0)
      .reduce((x, y) => (x._1, x._2 + y._2))
      .output(new OutputFormat[(String, Int)] {
        override def configure(configuration: Configuration): Unit = {}

        override def writeRecord(it: (String, Int)): Unit = {
          println("output2: " + it._1 + " " + it._2)
        }

        override def close(): Unit = {}

        override def open(i: Int, i1: Int): Unit = {}

      })
    
    env.execute()

  }
}
