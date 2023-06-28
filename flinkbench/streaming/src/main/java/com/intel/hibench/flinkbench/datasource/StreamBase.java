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

package com.intel.hibench.flinkbench.datasource;

import com.intel.hibench.flinkbench.util.FlinkBenchConfig;
import com.intel.hibench.flinkbench.util.KeyedTupleSchema;
import com.intel.hibench.flinkbench.util.StringTupleSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public abstract class StreamBase {

  private KafkaSource<Tuple2<String, String>> dataStream;

  public KafkaSource<Tuple2<String, String>> getDataStream() {
    return this.dataStream;
  }

  public void createDataStream(FlinkBenchConfig config) throws Exception {

    this.dataStream = KafkaSource.<Tuple2<String, String>>builder()
            .setBootstrapServers(config.brokerList)
            .setTopics(config.topic)
            .setGroupId(config.consumerGroup)
            .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)) // 从上次提交的位置开始消费，防止重复消费和丢失数据；如果该消费者组从未消费过那么从最开始消费
            .setDeserializer(new KeyedTupleSchema())
            .setProperty("partition.discovery.interval.ms", "10000") // 每隔十秒监测是否有新分区，老版本kafka不支持
            .setProperty("enable.auto.commit", "true")
            .setProperty("auto.commit.interval.ms", "1000") // 防止作业断开时未checkpoint提交offset导致重启作业重复消费的情况，设置自动提交offset间隔
            .build();
  }

  public void processStream(FlinkBenchConfig config) throws Exception {
  }
}
