package com.alex.space.spark.mains

import org.apache.spark.SparkContext

object Worker extends BaseWorker {

  def main(args: Array[String]): Unit = {
    println("input:" + configString("input"))

    val sparkContext = new SparkContext(sparkConf)


    new DataFrameDemo().run(sparkContext)
    new DataFrameDemo().runAsRdd(sparkContext)
    new DataFrameDemo().runAsSchema(sparkContext)

    logger.info("Finish")
  }


}
