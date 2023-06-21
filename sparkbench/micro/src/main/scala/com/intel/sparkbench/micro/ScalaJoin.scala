/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.sparkbench.micro

import org.apache.spark.{SparkConf, SparkContext}

/*
 * 监测数据倾斜情况下的join效率，数据生成参考ScalaJoinPrepare
 */
object ScalaJoin{
    def main(args: Array[String]){
        if (args.length < 2){
            System.err.println(
            s"Usage: $ScalaJoin <INPUT_HDFS> <OUTPUT_HDFS>"
            )
            System.exit(1)
        }
        val skewInput = args(0) + "/skew"
        val joinInput = args(0) + "/join"
        val output = args(1)
        val separate = ","

        val sparkConf = new SparkConf().setAppName("ScalaJoin")
        val sc = new SparkContext(sparkConf)

        val skewData = sc.textFile(skewInput).mapPartitions(lines => {
            lines.filter(_.nonEmpty).map(line => {
                val split = line.split(separate)
                (split(0), split(1))
            })
        })
        val joinData = sc.textFile(joinInput).mapPartitions(lines => {
            lines.filter(_.nonEmpty).map(line => {
                val split = line.split(separate)
                (split(0), split(1))
            })
        }).reduceByKey(_ + _)

        skewData.join(joinData).saveAsTextFile(output)
        sc.stop()
    }
}
