package com.intel.hibench.flinkbench.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import scala.Tuple2;

public class Join {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: [INPUT] [OUTPUT]");
        }
        String skewInput = args[0] + "/skew";
        String joinInput = args[0] + "/join";
        String output = args[1];
        String separate = ",";

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamingFileSink<Tuple2<String, String>> sink = StreamingFileSink.forRowFormat(new Path(output), new SimpleStringEncoder<Tuple2<String, String>>()).build();

        SingleOutputStreamOperator<Tuple2<String, String>> skewStream = environment.readTextFile(skewInput)
                .map((MapFunction<String, Tuple2<String, String>>) x -> {
                    String[] split = x.split(separate, -1);
                    return new Tuple2<>(split[0], split[1]);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        SingleOutputStreamOperator<Tuple2<String, String>> joinStream = environment.readTextFile(joinInput)
                .map((MapFunction<String, Tuple2<String, String>>) x -> {
                    String[] split = x.split(separate, -1);
                    return new Tuple2<>(split[0], split[1]);
                })
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}))
                .keyBy(Tuple2::_1)
                .reduce((ReduceFunction<Tuple2<String, String>>) (x, y) -> new Tuple2<>(x._1, x._2 + " " + y._2))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

        skewStream.join(joinStream)
                        .where(Tuple2::_1)
                        .equalTo(Tuple2::_1)
                        .window(GlobalWindows.create())
                        .apply((JoinFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>) (x, y) -> new Tuple2<>(x._1, x._1 + " " +  y._2),
                                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}))
                        .addSink(sink);

        try {
            environment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
