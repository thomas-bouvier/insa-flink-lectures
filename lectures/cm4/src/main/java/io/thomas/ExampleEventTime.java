package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class ExampleEventTime {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
		
		DataStream<Tuple3<Integer, Double, Long>> outputStream = dataStream.map(new FormatData())
																		   .assignTimestampsAndWatermarks(new MyTimeStampExtractor())
																		   .keyBy(0)
																		   .window(TumblingEventTimeWindows.of(Time.seconds(5)))
																		   .sum(1);

		
		// emit result
		outputStream.writeAsText("example-event-time.txt");
		// execute program
		env.execute("Streaming ExampleEventTime");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	
	public static class FormatData implements MapFunction<String, Tuple3<Integer, Double, Long>> {
		@Override
		public Tuple3<Integer, Double, Long> map(String value) throws Exception {
			return Tuple3.of(Integer.parseInt(value.split("  ")[0].trim()), 
							 Double.parseDouble(value.split("  ")[1].trim()),
							 Long.parseLong(value.split("  ")[2].trim()));
		}
	}
	
	public static class MyTimeStampExtractor implements AssignerWithPunctuatedWatermarks<Tuple3<Integer, Double, Long>> {
		@Override
		public long extractTimestamp(Tuple3<Integer, Double, Long> element, long previousElementTimestamp) {
			return element.f2;
		}
		@Override
		public Watermark checkAndGetNextWatermark(Tuple3<Integer, Double, Long> lastElement, long extractedTimestamp) {
			return new Watermark(lastElement.f2);
		}
	}
	
}
