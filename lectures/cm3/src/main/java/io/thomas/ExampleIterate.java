package io.thomas;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleIterate" -q
 */
public class ExampleIterate {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		
		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<Integer> dataStream = env.fromElements(12, 19, 30, 47);

		IterativeStream<Tuple2<Integer, Integer>> iterativeStream = dataStream.map(new addIterateCounter()).iterate();
																			  
		DataStream<Tuple2<Integer, Integer>> iterationBody = iterativeStream.map(new checkMultiple());

		DataStream<Tuple2<Integer, Integer>> feedback = iterationBody.filter(new MyFilterNotMultiple());
		iterativeStream.closeWith(feedback);

		DataStream<Tuple2<Integer, Integer>> outputStream = iterationBody.filter(new MyFilterMultiple());
		outputStream.print();

		// execute program
		env.execute("Streaming ExampleIterate");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class addIterateCounter implements MapFunction<Integer, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Integer value) throws Exception {
			return Tuple2.of(value, 0);
		}
	}

	public static class checkMultiple implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
		@Override
		public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> input) throws Exception {
			Tuple2<Integer, Integer> output;
			if (input.f0 % 4 == 0)
				output = input;
			else
				output = Tuple2.of(input.f0 - 1, input.f1 + 1);
			return output;
		}
	}

	public static class MyFilterNotMultiple implements FilterFunction<Tuple2<Integer, Integer>> {
		@Override
		public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
			return value.f0 % 4 != 0;
		}
	}
	
	public static class MyFilterMultiple implements FilterFunction<Tuple2<Integer, Integer>> {
		@Override
		public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
			return value.f0 % 4 == 0;
		}
	}

}
