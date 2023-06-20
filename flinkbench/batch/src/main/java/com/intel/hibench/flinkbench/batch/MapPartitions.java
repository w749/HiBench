package com.intel.hibench.flinkbench.batch;

import com.intel.sparkbench.micro.ScalaJoinPrepare;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapPartitions {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: [INPUT] [OUTPUT]");
        }
        String input = args[0];
        String output = args[1];

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path(output), new SimpleStringEncoder<String>()).build();

        List<String> resource = Arrays.stream(ScalaJoinPrepare.WORDS_ARRAY()).limit(ScalaJoinPrepare.WORDS_ARRAY().length / 2).collect(Collectors.toList());

        environment.readTextFile(input)
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                        Arrays.stream(value.split(" ", -1)).forEach(x -> {
                            if (resource.contains(x)) out.collect(x);
                        });
                    }
                })
                .returns(TypeInformation.of(new TypeHint<String>() {}))
                .addSink(sink);

        try {
            environment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
