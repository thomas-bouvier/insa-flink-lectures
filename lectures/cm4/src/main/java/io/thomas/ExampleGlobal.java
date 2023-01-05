package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

import io.thomas.ExampleSession.FormatData;
import io.thomas.ExampleSession.SumTemperature;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducerInactivity" -q
 * mvn install exec:java -Dmain.class="io.thomas.ExampleGlobal" -q
 */
public class ExampleGlobal {
	
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
		
		DataStream<Tuple2<Integer, Double>> outputStream = dataStream.map(new FormatData())
																	 .windowAll(GlobalWindows.create()).trigger(CountTrigger.of(5))
																	 .reduce(new SumTemperature());
		
		
		// DataStream<Tuple2<Integer, Double>> outputStream = dataStream.map(new FormatData())
		// 															 .keyBy(0)
		// 															 .window(GlobalWindows.create()).trigger(CountTrigger.of(5))
		// 															 .reduce(new SumTemperature());
		

		// emit result
		outputStream.print();
		// execute program
		env.execute("Streaming ExampleGlobal");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************
	
	public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
		@Override
		public Tuple2<Integer, Double> map(String value) throws Exception {
			return Tuple2.of(Integer.parseInt(value.split(" ")[0].trim()), 
							 Double.parseDouble(value.split(" ")[2].trim()));
		}
	}
	
	public static class SumTemperature implements ReduceFunction<Tuple2<Integer, Double>> {
		@Override
		public Tuple2<Integer, Double> reduce(
				Tuple2<Integer, Double> mycumulative,
				Tuple2<Integer, Double> input) throws Exception {
			return new Tuple2<>(
						input.f0, /* id */
						mycumulative.f1 + input.f1 /* temperature */
			);
		}
	}
	
}
