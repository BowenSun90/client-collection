package com.alex.space.spark.utils

object Utils {
  implicit def any2Int(x: Any): Int = x.toString.toInt
}
