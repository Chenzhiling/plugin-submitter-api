package com.czl.template

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2023/3/17
 * Description:
 */
object SparkStreamTest {

  def main(args: Array[String]): Unit = {

    val localhost: String = args(0)
    val port: String = args(1)

    val spark: SparkSession = SparkSession.builder().getOrCreate()
    val frame: DataFrame = spark.readStream.format("socket")
      .option("host", localhost)
      .option("port", port)
      .load()
    val query: StreamingQuery = frame.writeStream.format("console").start()
    query.awaitTermination()
  }
}
