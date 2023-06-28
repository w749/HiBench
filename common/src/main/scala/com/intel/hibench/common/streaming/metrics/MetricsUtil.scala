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

import com.intel.hibench.common.streaming.Platform
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.{Collections, Properties}

object MetricsUtil {

  val TOPIC_CONF_FILE_NAME = "metrics_topic.conf"

  def getTopic(platform: Platform, sourceTopic: String, producerNum: Int,
               recordPerInterval: Long, intervalSpan: Int): String = {
    val topic = s"${platform}_${sourceTopic}_${producerNum}_${recordPerInterval}" +
      s"_${intervalSpan}_${System.currentTimeMillis()}"
    println(s"metrics is being written to kafka topic $topic")
    topic
  }

  def createTopic(bootstrapServers: String, topic: String, partitions: Int): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, topic)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val adminClient = AdminClient.create(props)
    try {
      if (!adminClient.listTopics().names().get().contains(topic)) {
        val newTopic = new NewTopic(topic, partitions, 1)
        adminClient.createTopics(Collections.singleton(newTopic))
        while (!adminClient.listTopics().names().get().contains(topic)) {
          Thread.sleep(100)
        }
      }
    } catch {
      case e: Exception =>
        throw e
    } finally {
      adminClient.close()
    }
  }
}
