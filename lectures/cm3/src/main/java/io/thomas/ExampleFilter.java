package io.thomas;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ExampleFilter {
	
	public static final String[] WORDS = new String[] {
			"This", "is", "a", "simple", "example", "that", "shows", "how", "transformations", "work", "in", "Flink"
	};


	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> dataStream;
		if (params.has("input")) {
			// read the text file from given input path
			dataStream = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Executing WordCount example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			dataStream = env.fromElements(WORDS);
		}

		DataStream<String> outputStream = dataStream.filter(new RemoveShortWords());

		// emit result
		if (params.has("output")) {
			outputStream.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			outputStream.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class RemoveShortWords implements FilterFunction<String> {
		@Override
		public boolean filter(String word) throws Exception {
			return word.length()>3;
		}
	}

}
