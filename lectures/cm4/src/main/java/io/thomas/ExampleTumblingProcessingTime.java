package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducer" -q
   or nc -lk 9090
 * mvn install exec:java -Dmain.class="io.thomas.ExampleTumblingProcessingTime" -q
 */
public class ExampleTumblingProcessingTime {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
        
        DataStream<Tuple2<Integer, Double>> outputStream = dataStream.map(new FormatData())
                                                                     .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                                                                     .reduce(new SumTemperature());

        // emit result
        outputStream.print();
        // execute program
        env.execute("Streaming ExampleTumblingProcessingTime");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    
    public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) {
            return Tuple2.of(Integer.parseInt(value.split(" ")[0].trim()), 
                             Double.parseDouble(value.split(" ")[2].trim()));
        }
    }
    
    public static class SumTemperature implements ReduceFunction<Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> reduce(
                Tuple2<Integer, Double> mycumulative,
                Tuple2<Integer, Double> input) {
            return new Tuple2<>(
                        input.f0, /* id */
                        mycumulative.f1 + input.f1 /* temperature */
            );
        }
    }
}
