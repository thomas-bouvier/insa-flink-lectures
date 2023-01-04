package io.thomas;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleUnions" -Dexec.args="--input1 countries-stream.txt --input2 countries-stream-v2.txt" -q
 */
public class ExampleUnions {

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> stream1 = env.readTextFile(params.get("input1"));
		DataStream<String> stream2 = env.readTextFile(params.get("input2"));
		
		DataStream<String> countriesUnion = stream1.union(stream2).map(new ExtractCountries())
										       					  .flatMap(new SplitCountries());

		
		// emit result
		if (params.has("output")) {
			countriesUnion.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			countriesUnion.print();
		}

		// execute program
		env.execute("Streaming ExampleUnions");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class ExtractCountries implements MapFunction<String, String> {
		@Override
		public String map(String continent) throws Exception {
			return continent.split("\t")[1];
		}
	}
	
	public static class SplitCountries implements FlatMapFunction<String, String> {
		@Override
		public void flatMap(String input, Collector<String> out) throws Exception {
			String[] countries = input.split(",");
			for (String country : countries) {
				out.collect(country.trim());
			}
		}
	}

}
