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

package com.intel.hibench.sparkbench.streaming.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.storage.StorageLevel

case class SparkBenchConfig (
  // Spark
  master: String,
  benchName: String,
  batchInterval: Int,
  batchMaxMessagePartition: Long,
  receiverNumber: Int,
  copies: Int,
  enableWAL: Boolean,
  checkpointPath: String,
  directMode: Boolean,

  // Kafka
  zkHost: String,
  consumerGroup: String,
  sourceTopic: String,
  reporterTopic: String,
  brokerList: String,


  // Hibench
  debugMode: Boolean,
  coreNumber: Int,
  sampleProbability: Double,
  windowDuration: Long,
  windowSlideStep: Long) {

  def storageLevel = copies match {
    case 0 => StorageLevel.MEMORY_ONLY
    case 1 => StorageLevel.MEMORY_AND_DISK_SER
    case _ => StorageLevel.MEMORY_AND_DISK_SER_2
  }

  def kafkaParams = Map (
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean),
    ConsumerConfig.GROUP_ID_CONFIG  -> consumerGroup
  )

  def threadsPerReceiver = coreNumber / receiverNumber
}
