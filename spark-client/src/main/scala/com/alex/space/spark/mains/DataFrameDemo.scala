package com.alex.space.spark.mains

import com.alex.space.spark.pojo.Person
import org.apache.spark.SparkContext


/**
  * Created by Alex on 2017/7/24.
  */
class DataFrameDemo {

  // DataFrame
  def run(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.json("hdfs://localhost:9000/data/people.json")
    df.show()
    df.printSchema()
    df.select("name").show()
    df.filter(df("age") > 21).show()
    df.groupBy("age").count().show()

    df.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    teenagers.map(t => "Name:" + t.getAs[String]("name")).foreach(println)
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).foreach(println)
  }

  // 采用反映机制进行Schema类型推导
  def runAsRdd(sc: SparkContext) = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val people = sc.textFile("hdfs://localhost:9000/data/people.txt")
      .map(_.split(","))
      .map(p => Person(p(0), p(1).trim.toInt))
      .toDF()

    people.registerTempTable("people")
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }

  // 利用程序动态指定Schema
  def runAsSchema(sc: SparkContext): Unit = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Schema字符串
    val schemaString = "name,age"

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StringType, StructField, StructType}

    //利用schemaString动态生成Schema
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true)))

    // 将people RDD转换成Rows
    val rowRDD = sc.textFile("hdfs://localhost:9000/data/people.txt")
      .map(_.split(","))
      .map(p => Row(p(0), p(1).trim))

    // 创建DataFrame
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    peopleDataFrame.registerTempTable("people")
    val results = sqlContext.sql("SELECT name FROM people")
    results.map(t => "Name: " + t(0)) foreach println

  }

}
