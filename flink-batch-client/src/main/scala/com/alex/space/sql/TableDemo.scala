package com.alex.space.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

/**
  * @author Alex
  *         Created by Alex on 2019/3/1.
  */
object TableDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val inputFile = "data/hits-log.csv"
    val joinFile = "data/ip-src.csv"

    // CsvTableSource
    val fieldNames: Array[String] = Array("ip", "sessionId", "url", "ts")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.STRING, Types.LONG)

    val fromSource: CsvTableSource = new CsvTableSource(
      inputFile,
      fieldNames,
      fieldTypes,
      ",",
      "\n"
    )

    tableEnv.registerTableSource(s"hits", fromSource)

    // TextFileInput
    val dataSet = env.readTextFile(joinFile)
      .map(x => x.split(","))
      .map(x => (x(0), x(1)))

    tableEnv.registerDataSet("ipInfo", dataSet, 'ip1, 'src)


    // Demo: Select
    val table1 = tableEnv.sqlQuery("SELECT ip,sessionId,ts FROM hits")

    implicit val tpe1: TypeInformation[Row] = new RowTypeInfo(table1.getSchema.getTypes,
      table1.getSchema.getColumnNames)

    table1.toDataSet[Row](tpe1).print()

    println("============")

    // Demo: Group by
    val table2 = tableEnv.sqlQuery("SELECT sessionId, COUNT(*) FROM hits GROUP BY sessionId")

    implicit val tpe2: TypeInformation[Row] = new RowTypeInfo(table2.getSchema.getTypes,
      table2.getSchema.getColumnNames)

    table2.toDataSet[Row](tpe2).print()

    println("============")

    // Demo: Join
    val table3 = tableEnv.sqlQuery("SELECT ip, url FROM hits WHERE ts > 1551457138849 and ts < 1551457338849")
    tableEnv.registerTable("t1", table3)

    implicit val tpe3: TypeInformation[Row] = new RowTypeInfo(table3.getSchema.getTypes,
      table3.getSchema.getColumnNames)

    table3.toDataSet[Row](tpe3).print()

    println("============")

    val table4 = tableEnv.sqlQuery("SELECT src, COUNT(*) from t1 left join ipInfo on t1.ip = ipInfo.ip1 group by src")

    implicit val tpe4: TypeInformation[Row] = new RowTypeInfo(table4.getSchema.getTypes,
      table4.getSchema.getColumnNames)

    table4.toDataSet[Row](tpe4).print()

    println("============")


  }
}
