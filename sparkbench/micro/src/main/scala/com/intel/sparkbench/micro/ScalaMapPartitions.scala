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
 * 测试MapPartitions，和Join使用的是一个数据集
 */
object ScalaMapPartitions{
    def main(args: Array[String]){
        if (args.length < 2){
            System.err.println(
            s"Usage: $ScalaJoin <INPUT_HDFS> <OUTPUT_HDFS>"
            )
            System.exit(1)
        }
        val input = args(0)
        val output = args(1)
        val separate = " "

        val sparkConf = new SparkConf().setAppName("ScalaJoin")
        val sc = new SparkContext(sparkConf)


        val joinDataBC = sc.broadcast(ScalaJoinPrepare.WORDS_ARRAY.take(ScalaJoinPrepare.WORDS_ARRAY.length / 2))

        sc.textFile(input).mapPartitions(lines => {
            lines.filter(_.nonEmpty).filter(line => {
                val split = line.split(separate)
                val key = split(0)
                joinDataBC.value.contains(key)
            })
        }).saveAsTextFile(output)

        sc.stop()
    }
}
