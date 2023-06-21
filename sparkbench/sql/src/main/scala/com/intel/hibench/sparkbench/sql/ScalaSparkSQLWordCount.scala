package com.intel.hibench.sparkbench.sql

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ScalaSparkSQLWordCount {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println(
                s"Usage: $ScalaSparkSQLWordCount <INPUT_HDFS> <OUTPUT_HDFS>"
            )
            System.exit(1)
        }
        val input = args(0)
        val output = args(1)

        val conf = new SparkConf()
        conf.set("spark.sql.adaptive.enabled", "true")
          .set("spark.sql.adaptive.skewJoin.enabled", "true")
        val session = SparkSession
          .builder()
          .config(conf)
          .appName("ScalaSparkSQLWordCount")
          .getOrCreate()

        import session.implicits._
        val io = new IOCommon(session.sparkContext)
        val data = io.load[String](input)
          .toDS()
          .flatMap(_.split(" "))
        data.createTempView("tmp")
        session.sql("select value, count(*) counts from tmp group by value")
          .write
          .csv(output)
        session.stop()
    }

}
