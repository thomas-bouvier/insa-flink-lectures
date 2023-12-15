package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducerTimeStamp" -q
 * mvn install exec:java -Dmain.class="io.thomas.ExampleEventTime" -q
 */
public class ExampleTumblingEventTimeWatermarks {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);

        DataStream<Tuple3<Integer, Double, Long>> formattedData = dataStream.map(new FormatData());

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<Tuple3<Integer, Double, Long>> watermarkStrategy =
                WatermarkStrategy.<Tuple3<Integer, Double, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (t, timestamp) -> t.f2);

        DataStream<Tuple3<Integer, Double, Long>> outputStream = formattedData.assignTimestampsAndWatermarks(watermarkStrategy)
                                                                           .keyBy(t -> t.f0)
                                                                           .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                                                                           .sum(1);

        
        // emit result
        outputStream.print();
        // execute program
        env.execute("Streaming ExampleEventTime");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    
    public static class FormatData implements MapFunction<String, Tuple3<Integer, Double, Long>> {
        @Override
        public Tuple3<Integer, Double, Long> map(String value) {
            // Forcing the key to be 0 for this example
            return Tuple3.of(0, 
                             Double.parseDouble(value.split(" ")[2].trim()),
                             Long.parseLong(value.split(" ")[1].trim()));
        }
    }
    
}
