package com.alex.space.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Word count example
  */
object WordCount {
  def main(args: Array[String]) {
    /**
      * 第1步；创建Spark的配置对象SparkConf，设置Spark程序运行时的配置信息
      * 例如 setAppName用来设置应用程序的名称，在程序运行的监控界面可以看到该名称，
      * setMaster设置程序运行在本地还是运行在集群中，运行在本地可是使用local参数，也可以使用local[K]/local[*],
      * 可以去spark官网查看它们不同的意义。 如果要运行在集群中，以Standalone模式运行的话，需要使用spark://HOST:PORT
      * 的形式指定master的IP和端口号，默认是7077
      */
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    /**
      * 第2步：创建SparkContext 对象
      * SparkContext是Spark程序所有功能的唯一入口
      * SparkContext核心作用： 初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
      * 同时还会负责Spark程序往Master注册程序
      *
      * 通过传入SparkConf实例来定制Spark运行的具体参数和配置信息
      */
    val sc = new SparkContext(conf)

    /**
      * 第3步： 根据具体的数据来源(HDFS、 HBase、Local FS、DB、 S3等)通过SparkContext来创建RDD
      * RDD 的创建基本有三种方式： 根据外部的数据来源(例如HDFS)、根据Scala集合使用SparkContext的parallelize方法、
      * 由其他的RDD操作产生
      * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */
    val lines = sc.textFile("README.MD") // 读取本地文件
    //  val lines = sc.textFile("/library/wordcount/input")   // 读取HDFS文件，并切分成不同的Partition
    //  val lines = sc.textFile("hdfs://master:9000/libarary/wordcount/input")  // 或者明确指明是从HDFS上获取数据

    /**
      * 第4步： 对初始的RDD进行Transformation级别的处理，例如 map、filter等高阶函数来进行具体的数据计算
      */
    val words = lines.flatMap(_.split(" ")).filter(w => w != "")
    val pairs = words.map(w => (w, 1))
    val wordCount = pairs.reduceByKey(_ + _)
    //  val wordscount = pairs.reduceByKey((v1, v2) => v1 + v2)  // 等同于

    // 打印结果，使用collect会将集群中的数据收集到当前运行drive的机器上，需要保证单台机器能放得下所有数据
    wordCount.collect.foreach(println)

    sc.stop() // 释放资源
  }
}