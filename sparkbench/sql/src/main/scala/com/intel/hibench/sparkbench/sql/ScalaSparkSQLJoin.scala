package com.intel.hibench.sparkbench.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ScalaSparkSQLJoin {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println(
                s"Usage: $ScalaSparkSQLJoin <INPUT_HDFS> <OUTPUT_HDFS>"
            )
            System.exit(1)
        }
        val skewInput = args(0) + "/skew"
        val joinInput = args(0) + "/join"
        val output = args(1)
        val separate = ","

        val conf = new SparkConf()
        conf.set("spark.sql.adaptive.enabled", "true")
          .set("spark.sql.adaptive.skewJoin.enabled", "true")
        val session = SparkSession
          .builder()
          .config(conf)
          .appName("ScalaSparkSQLJoin")
          .getOrCreate()

        import session.implicits._
        session.read.textFile(skewInput).mapPartitions(lines => {
            lines.filter(_.nonEmpty).map(line => {
                val split = line.split(separate)
                (split(0), split(1))
            })
        }).toDF("key1", "value1")
          .createTempView("skewData")
        session.read.textFile(joinInput).mapPartitions(lines => {
            lines.filter(_.nonEmpty).map(line => {
                val split = line.split(separate)
                (split(0), split(1))
            })
        }).toDF("key2", "value2")
          .createTempView("joinData")

        session.sql("select key1 as key, concat_ws(' ', value1, value2) as value from skewData inner join joinData on skewData.key1 = joinData.key2")
          .write
          .csv(output)

        session.stop()
    }
}
