package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ExampleCustom {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
		
		DataStream<Tuple4<Integer, Integer, Double, Double>> outputStream = dataStream.map(new FormatData())
																					  .keyBy(0)
																					  .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
																					  .apply(new MyWindowFunction());

		// emit result
		outputStream.writeAsText("example-custom.txt");
		// execute program
		env.execute("Streaming ExampleCustom");
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
	
	public static class MyWindowFunction implements WindowFunction<Tuple2<Integer, Double>, Tuple4<Integer, Integer, Double, Double>, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple key, 
						  TimeWindow window,
						  Iterable<Tuple2<Integer, Double>> input,
						  Collector<Tuple4<Integer, Integer, Double, Double>> out) throws Exception {
			int my_id = 0, count = 0;
			double sum = 0, avg = 0;
			for (Tuple2<Integer, Double> event: input) {
				my_id = event.f0;
				count+=1;
				sum+= event.f1;
			}
			avg = sum / count;
			out.collect(Tuple4.of(my_id, count, sum, avg));
		}
	}
	
//	public static class MyWindowFunction implements WindowFunction<Tuple2<Integer, Double>, HashMap<Integer, Integer>, Tuple, GlobalWindow> {
//		@Override
//		public void apply(Tuple key, 
//						  GlobalWindow window,
//						  Iterable<Tuple2<Integer, Double>> input,
//						  Collector<HashMap<Integer, Integer>> out) throws Exception {
//			
//			HashMap<Integer, Integer> myStats = new HashMap<Integer, Integer>();
//			
//			for (Tuple2<Integer, Double> event: input) {
//				if (!myStats.containsKey(event.f0)) {
//					myStats.put(event.f0, 1);
//				} else {
//					myStats.put(event.f0, myStats.get(event.f0) + 1);
//				}
//			}
//			out.collect(myStats);
//		}
//		
//	}
	
//	public static class SumTemperature implements ReduceFunction<Tuple2<Integer, Double>> {
//		@Override
//		public Tuple2<Integer, Double> reduce(
//				Tuple2<Integer, Double> mycumulative,
//				Tuple2<Integer, Double> input) throws Exception {
//			return new Tuple2<Integer, Double>(
//						input.f0, /* id */
//						mycumulative.f1 + input.f1 /* temperature */
//			);
//		}
//	}
	
}
