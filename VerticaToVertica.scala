package org.test

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
//import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
//import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala.ExecutionEnvironment
//import org.apache.flink.table.api.java.BatchTableEnvironment

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

object VtV {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)
    //Read mysql
    val dataSource = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.vertica.jdbc.Driver")
      .setDBUrl("jdbc:vertica://172.16.67.161:5433/vertica_db?user=release&password=vdb")
      .setQuery("select * from team")
      .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
      .finish())

    tableEnv.registerDataSet("tb01", dataSource);

    val query = tableEnv.sqlQuery("select * from tb01")

    val result = tableEnv.toDataSet[Row](query)

//    result.print()
//    println(result)

    result.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.vertica.jdbc.Driver")
      .setDBUrl("jdbc:vertica://172.16.67.161:5433/vertica_db?user=release&password=vdb")
      .setQuery("insert into team2 (id,name, address) values (?,?,?)")
      .setSqlTypes(Array(java.sql.Types.VARCHAR, java.sql.Types.VARCHAR, java.sql.Types.VARCHAR))
      .finish());

    env.execute("flink-test");
  }

}
