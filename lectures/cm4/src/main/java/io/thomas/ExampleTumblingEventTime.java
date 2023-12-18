package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducerTimeStamp" -q
 * mvn install exec:java -Dmain.class="io.thomas.ExampleTumblingEventTime" -q
 */
public class ExampleTumblingEventTime {
    
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
                                                                            .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                                                                            .sum(1);

        // emit result
        outputStream.print();
        // execute program
        env.execute("Streaming ExampleTumblingEventTime");
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
    
}
