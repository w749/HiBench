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

package com.intel.hibench.common.streaming.metrics

import com.codahale.metrics.{Histogram, UniformReservoir}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.io.{File, FileWriter}
import java.util.concurrent.{Executors, Future, TimeUnit}
import java.util.{Date, Properties}
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer


class KafkaCollector(bootstrapServers: String, metricsTopic: String,
                     outputDir: String, sampleNumber: Int, desiredThreadNum: Int) extends LatencyCollector {

  private val histogram = new Histogram(new UniformReservoir(sampleNumber))
  private val threadPool = Executors.newFixedThreadPool(desiredThreadNum)
  private val fetchResults = ArrayBuffer.empty[Future[FetchJobResult]]

  def start(): Unit = {
    val partitions = getPartitions(metricsTopic, bootstrapServers)

    println("Starting MetricsReader for kafka topic: " + metricsTopic)

    partitions.foreach(partition => {
      val job = new FetchJob(bootstrapServers, metricsTopic, partition, histogram)
      val fetchFeature = threadPool.submit(job)
      fetchResults += fetchFeature
    })

    threadPool.shutdown()
    threadPool.awaitTermination(30, TimeUnit.MINUTES)

    val finalResults = fetchResults.map(_.get()).reduce((a, b) => {
      val minTime = Math.min(a.minTime, b.minTime)
      val maxTime = Math.max(a.maxTime, b.maxTime)
      val count = a.count + b.count
      new FetchJobResult(minTime, maxTime, count)
    })

    report(finalResults.minTime, finalResults.maxTime, finalResults.count)
  }

  private def getPartitions(topic: String, bootstrapServers: String): Seq[Int] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, topic)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    new KafkaConsumer(props)
      .partitionsFor(topic)
      .asScala
      .map(_.partition())
  }


  private def report(minTime: Long, maxTime: Long, count: Long): Unit = {
    val outputFile = new File(outputDir, metricsTopic + ".csv")
    println(s"written out metrics to ${outputFile.getCanonicalPath}")
    val header = "time,count,throughput(msgs/s),max_latency(ms),mean_latency(ms),min_latency(ms)," +
        "stddev_latency(ms),p50_latency(ms),p75_latency(ms),p95_latency(ms),p98_latency(ms)," +
        "p99_latency(ms),p999_latency(ms)\n"
    val fileExists = outputFile.exists()
    if (!fileExists) {
      val parent = outputFile.getParentFile
      if (!parent.exists()) {
        parent.mkdirs()
      }
      outputFile.createNewFile()
    }
    val outputFileWriter = new FileWriter(outputFile, true)
    if (!fileExists) {
      outputFileWriter.append(header)
    }
    val time = new Date(System.currentTimeMillis()).toString
    val count = histogram.getCount
    val snapshot = histogram.getSnapshot
    val throughput = count * 1000 / (maxTime - minTime)
    outputFileWriter.append(s"$time,$count,$throughput," +
        s"${formatDouble(snapshot.getMax)}," +
        s"${formatDouble(snapshot.getMean)}," +
        s"${formatDouble(snapshot.getMin)}," +
        s"${formatDouble(snapshot.getStdDev)}," +
        s"${formatDouble(snapshot.getMedian)}," +
        s"${formatDouble(snapshot.get75thPercentile())}," +
        s"${formatDouble(snapshot.get95thPercentile())}," +
        s"${formatDouble(snapshot.get98thPercentile())}," +
        s"${formatDouble(snapshot.get99thPercentile())}," +
        s"${formatDouble(snapshot.get999thPercentile())}\n")
    outputFileWriter.close()
  }

  private def formatDouble(d: Double): String = {
    "%.3f".format(d)
  }

}


