package org.test

import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import java.sql.PreparedStatement

case class DatatypeBinding(id: String, name: String, address: String)

object FlinkWithVerticaDemo {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val verticaConn = new JdbcConnectionOptionsBuilder()
      .withUrl("jdbc:vertica://172.16.67.161:5433/vertica_db?user=release&password=vdb")
      .withDriverName("com.vertica.jdbc.Driver")
      .build()


    val ds = env.fromElements(DatatypeBinding("4400126", "Tom", "Add1"),
      DatatypeBinding("4400127", "Kim", "Add2"),
      DatatypeBinding("4400128", "James", "Add3"),
      DatatypeBinding("4400129", "Kerry", "Add4"))

    val statementBuilder = new JdbcStatementBuilder[DatatypeBinding] {
      override def accept(ps: PreparedStatement, t: DatatypeBinding): Unit = {
        ps.setString(1, t.id)
        ps.setString(2, t.name)
        ps.setString(3, t.address)
      }
    }

    ds
      .addSink(
        JdbcSink.sink(
          String.format("insert into %s (id, name, address) values (?,?,?)", "team"),
          statementBuilder,
          verticaConn
        )
      )
    env.execute()
  }

}

