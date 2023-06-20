package com.intel.hibench.flinkbench.batch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Usage: [INPUT] [OUTPUT]");
        }
        String input = args[0];
        String output = args[1];

        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);

        Job job;
        try {
            job = Job.getInstance();
            FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(input));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // createInput不支持批处理
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        environment.getCheckpointConfig().configure(configuration);
//        environment.createInput(HadoopInputs.createHadoopInput(new SequenceFileInputFormat<NullWritable, Text>(), NullWritable.class, Text.class, job))
//                .map((MapFunction<org.apache.flink.api.java.tuple.Tuple2<NullWritable, Text>, String>) value -> String.valueOf(value.f1))
//                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, collector) -> Arrays.stream(value.split(" ")).forEach(x -> collector.collect(new Tuple2<>(x, 1L))))
//                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
//                .keyBy((KeySelector<Tuple2<String, Long>, String>) value -> value._1)
//                .reduce((ReduceFunction<Tuple2<String, Long>>) (x, y) -> new Tuple2<>(x._1, x._2 + y._2))
//                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
//                .addSink(sink);
//
//        try {
//            environment.execute();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }

        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.configure(configuration, executionEnvironment.getUserCodeClassLoader());
        executionEnvironment.createInput(HadoopInputs.createHadoopInput(new SequenceFileInputFormat<NullWritable, Text>(), NullWritable.class, Text.class, job))
                .map((MapFunction<org.apache.flink.api.java.tuple.Tuple2<NullWritable, Text>, String>) value -> String.valueOf(value.f1))
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, collector) -> Arrays.stream(value.split(" ")).forEach(x -> collector.collect(new Tuple2<>(x, 1L))))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .groupBy((KeySelector<Tuple2<String, Long>, String>) value -> value._1)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (x, y) -> new Tuple2<>(x._1, x._2 + y._2))
                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}))
                .writeAsText(output);

        try {
            executionEnvironment.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
