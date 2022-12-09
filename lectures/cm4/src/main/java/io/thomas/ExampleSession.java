package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ExampleSession {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
		
//		DataStream<Tuple2<Integer, Double>> outputStream = dataStream.map(new FormatData())
//																	 .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
//																	 .reduce(new SumTemperature());
		
		DataStream<Tuple2<Integer, Double>> outputStream = dataStream.map(new FormatData())
																	 .keyBy(0)
																	 .window(ProcessingTimeSessionWindows.withGap(Time.seconds(2)))
																	 .sum(1);

		// emit result
		outputStream.writeAsText("example-session.txt");
		// execute program
		env.execute("Streaming ExampleSession");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	
	public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
		@Override
		public Tuple2<Integer, Double> map(String value) throws Exception {
			return Tuple2.of(Integer.parseInt(value.split("  ")[0].trim()), 
							 Double.parseDouble(value.split("  ")[1].trim()));
		}
	}
	
	public static class SumTemperature implements ReduceFunction<Tuple2<Integer, Double>> {
		@Override
		public Tuple2<Integer, Double> reduce(
				Tuple2<Integer, Double> mycumulative,
				Tuple2<Integer, Double> input) throws Exception {
			return new Tuple2<Integer, Double>(
						input.f0, /* id */
						mycumulative.f1 + input.f1 /* temperature */
			);
		}
	}
	
}
