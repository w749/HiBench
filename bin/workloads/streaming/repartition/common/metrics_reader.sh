#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

current_dir=`dirname "$0"`
current_dir=`cd "$current_dir"; pwd`
root_dir=${current_dir}/../../../../..
workload_config=${root_dir}/conf/workloads/streaming/repartition.conf
. "${root_dir}/bin/functions/load_bench_config.sh"

enter_bench MetricsReader ${workload_config} ${current_dir}
show_bannar start

printFullLog

  ${STREAMING_KAFKA_HOME}/bin/kafka-topics.sh --zookeeper ${STREAMING_ZKADDR} --list

read -p "Please input the topic:" TOPIC

CMD="${JAVA_BIN} -cp ${COMMON_JAR} com.intel.hibench.common.streaming.metrics.MetricsReader ${STREAMING_BROKER_LIST} ${TOPIC} ${METRICS_READER_OUTPUT_DIR} ${METRICE_READER_SAMPLE_NUM} ${METRICS_READER_THREAD_NUM}"

execute_withlog $CMD

show_bannar finish
