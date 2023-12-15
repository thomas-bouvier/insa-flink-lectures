package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ExampleFunction {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
        
        DataStream<Tuple4<Integer, Integer, Double, Double>> outputStream = dataStream.map(new FormatData())
                                                                                      .keyBy(t -> t.f0)
                                                                                      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                                                                                      .process(new MyWindowFunction());

        // emit result
        outputStream.print();
        // execute program
        env.execute("Streaming ExampleCustom");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) {
            return Tuple2.of(0, 
                             Double.parseDouble(value.split(" ")[1].trim()));
        }
    }
    
    public static class MyWindowFunction extends ProcessWindowFunction<Tuple2<Integer, Double>, Tuple4<Integer, Integer, Double, Double>, Integer, TimeWindow> {
        @Override
        public void process(Integer key,
                            Context context,
                            Iterable<Tuple2<Integer, Double>> input,
                            Collector<Tuple4<Integer, Integer, Double, Double>> out) {
            int my_id = 0, count = 0;
            double sum = 0, avg;
            for (Tuple2<Integer, Double> event : input) {
                my_id = event.f0;
                count += 1;
                sum += event.f1;
            }
            avg = sum / count;
            out.collect(Tuple4.of(my_id, count, sum, avg));
        }
    }
}
