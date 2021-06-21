package org.test
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import java.util.Properties
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo

object FlinkReadVerticaDemo {
  def main(args: Array[String]): Unit = {
    readFromVertica()
  }

   def readFromVertica(): Unit = {
    val rowTypeInfo = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

    //Creates an execution environment that represents the context in which the program is currently executed.
    val env = ExecutionEnvironment.getExecutionEnvironment()

    val selectQuery = "select * from team"
    val driverName = "com.vertica.jdbc.Driver"
    val dbURL = "jdbc:vertica://172.16.67.161:5433/vertica_db?user=release&password=vdb"

    //Define Input Format Builder
    val inputBuilder =
      JDBCInputFormat.buildJDBCInputFormat().setDrivername(driverName).setDBUrl(dbURL)
        .setQuery(selectQuery).setRowTypeInfo(rowTypeInfo)

    //Get Data from SQL Table
    val source = env.createInput(inputBuilder.finish())

    //Print DataSet
    source.print();
  }
}
