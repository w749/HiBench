package com.intel.hibench.flinkbench.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class KeyedTupleSchema implements KafkaRecordDeserializationSchema<Tuple2<String, String>> {

  @Override
  public TypeInformation<Tuple2<String, String>> getProducedType() {
    return new TupleTypeInfo<Tuple2<String, String>>(TypeExtractor.createTypeInfo(String.class), TypeExtractor.createTypeInfo(String.class));
  }

  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Tuple2<String, String>> out) throws IOException {
    out.collect(new Tuple2<>(new String(record.key()), new String(record.value())));
  }
}

