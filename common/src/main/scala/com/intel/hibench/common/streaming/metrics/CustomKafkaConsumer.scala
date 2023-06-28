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

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties

class CustomKafkaConsumer(zookeeperConnect: String, topic: String, partition: Int) {
  private val CLIENT_ID = "metrics_reader"
  private val consumer: Consumer[String, String] = createConsumer

  def fetchRecords(): ConsumerRecords[String, String] = {
    val consumerRecords = consumer.poll(Duration.ofSeconds(1))
    consumer.commitAsync()
    consumerRecords
  }

  def close(): Unit = {
    consumer.close()
  }

  private def createConsumer: KafkaConsumer[String, String] = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, zookeeperConnect)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, CLIENT_ID)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

    val kafkaConsumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    val topicPartition = new TopicPartition(topic, partition)
    kafkaConsumer.assign(util.Arrays.asList(topicPartition))
    kafkaConsumer
  }
}
