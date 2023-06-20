package com.intel.hibench.flinkbench.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class Sort {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: [INPUT] [OUTPUT]");
        }
        String input = args[0];
        String output = args[1];

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamingFileSink<SortClass> sink = StreamingFileSink.forRowFormat(new Path(output), new SimpleStringEncoder<SortClass>()).build();

        environment.readTextFile(input)
                .map((MapFunction<String, SortClass>) x -> new SortClass(x, 1), TypeInformation.of(new TypeHint<SortClass>() {}))
                .shuffle()
                .keyBy(SortClass::getValue)
                .window(GlobalWindows.create())
                .apply((WindowFunction<SortClass, SortClass, String, GlobalWindow>) (key, window, values, out) ->
                        StreamSupport.stream(Spliterators.spliteratorUnknownSize(values.iterator(), Spliterator.ORDERED), false).sorted().forEach(out::collect),
                        TypeInformation.of(new TypeHint<SortClass>() {}))
                .addSink(sink);

        try {
            environment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class SortClass implements Serializable, Comparable<SortClass> {
        private String value;
        private Integer count;
        public SortClass(String value, Integer count) {
            this.value = value;
            this.count = count;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public String getValue() {
            return value;
        }

        public Integer getCount() {
            return count;
        }

        @Override
        public int compareTo(SortClass other) {
            return this.value.compareTo(other.value);
        }

        @Override
        public String toString() {
            return value + " " + count;
        }
    }
}
