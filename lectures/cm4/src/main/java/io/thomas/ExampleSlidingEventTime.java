package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducerTimeStamp" -q
 * or nc -lk 9090
 * mvn install exec:java -Dmain.class="io.thomas.ExampleSlidingEventTime" -q
 */
public class ExampleSlidingEventTime {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<Tuple3<Integer, Double, Long>> watermarkStrategy =
                WatermarkStrategy.<Tuple3<Integer, Double, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (t, timestamp) -> t.f2);


        DataStream<Tuple3<Integer, Double, Long>> outputStream = dataStream.map(new FormatData())
                                                                    .assignTimestampsAndWatermarks(watermarkStrategy)
                                                                    .windowAll(SlidingEventTimeWindows.of(Time.seconds(5),
                                                                                                          Time.seconds(3)))
                                                                    .reduce(new SumTemperature());

        // emit result
        outputStream.print();
        // execute program
        env.execute("Streaming ExampleSlidingEventTime");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    
    public static class FormatData implements MapFunction<String, Tuple3<Integer, Double, Long>> {
        @Override
        public Tuple3<Integer, Double, Long> map(String value) {
            return Tuple3.of(Integer.parseInt(value.split(" ")[0].trim()),
                            Double.parseDouble(value.split(" ")[2].trim()),
                            Long.parseLong(value.split(" ")[3].trim()));
        }
    }
    
    public static class SumTemperature implements ReduceFunction<Tuple3<Integer, Double, Long>> {
        @Override
        public Tuple3<Integer, Double, Long> reduce(
                Tuple3<Integer, Double, Long> mycumulative,
                Tuple3<Integer, Double, Long> input) {
            return new Tuple3<>(
                    input.f0, /* id */
                    mycumulative.f1 + input.f1 /* temperature */,
                    input.f2
            );
        }
    }
}
