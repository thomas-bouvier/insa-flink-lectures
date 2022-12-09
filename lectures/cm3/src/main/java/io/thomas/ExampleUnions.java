package io.thomas;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class ExampleUnions {
	

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataSet<String> dataset1 = env.readTextFile(params.get("input1"));
		DataSet<String> dataset2 = env.readTextFile(params.get("input2"));
		
		DataSet<String> countries1 = dataset1.map(new ExtractCountries())
										       .flatMap(new SplitCountries());

		DataSet<String> countries2 = dataset2.map(new ExtractCountries())
			       							   .flatMap(new SplitCountries());
		
		DataSet<String> countriesUnion = countries1.union(countries2);

		
		// emit result
		if (params.has("output")) {
			countriesUnion.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			countriesUnion.print();
		}

		// execute program
		env.execute("Streaming WordCount");
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
