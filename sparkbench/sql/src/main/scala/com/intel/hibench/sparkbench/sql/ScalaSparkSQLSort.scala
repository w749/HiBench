package com.intel.hibench.sparkbench.sql

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ScalaSparkSQLSort {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println(
                s"Usage: $ScalaSparkSQLSort <INPUT_HDFS> <OUTPUT_HDFS>"
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
          .appName("ScalaSparkSQLSort")
          .getOrCreate()

        import session.implicits._
        val io = new IOCommon(session.sparkContext)
        val data = io.load[String](input)
          .toDS()
          .flatMap(_.split(" "))
        data.sort("value")
          .write
          .csv(output)
        session.stop()
    }

}
