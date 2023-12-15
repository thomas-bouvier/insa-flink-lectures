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
public class ExampleTumblingEventTime {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);

        DataStream<Tuple3<Integer, Double, Long>> outputStream = dataStream.map(new FormatData())
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
