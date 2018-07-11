package com.alex.space.spark.mains

import com.alex.space.spark.config.Configable
import com.alex.space.spark.config.Constants.{ENV_LOCAL, ENV_PRODUCT}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

import scala.reflect.io.File

class BaseWorker extends LazyLogging with Configable {

  override val prefix: String = "default"

  def sparkConf: SparkConf = env match {
    case ENV_LOCAL => new SparkConf().setMaster("local[*]").setAppName("Worker")
    case ENV_PRODUCT => new SparkConf()
  }

  def outputPath: String = env match {
    case ENV_LOCAL => val path = configString("output")
      File(path).deleteRecursively()
      path
    case ENV_PRODUCT => val path = configString("output")
      FileSystem.get(new Configuration).delete(new Path(path), true)
      path
  }

}
