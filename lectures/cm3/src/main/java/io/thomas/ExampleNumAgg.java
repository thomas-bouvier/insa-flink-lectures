package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleNumAgg" -q
 */
public class ExampleNumAgg {
	
	public static final String[] WORDS = new String[] {
			"Europe	Belgium, 11539878, 0.147",
			"Europe	France, 67146000, 0.858",
			"Europe	Greece, 10724599, 0.137",
			"Europe	Spain, 47329981, 0.605",
			"Europe	Austria, 8935112, 0.114",
			"Europe	Romania, 19317984, 0.247",
			"Asia	China, 1405384280, 8.0",
			"Asia	Japan, 125880000, 1.61",
			"Asia	Malaysia, 32709470, 0.418",
			"Europe	Spain, 47329981, 0.605",
			"America	United States, 330674288, 4.22",
			"America	Brazil, 212356116, 2.71"
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
			System.out.println("Executing NumAgg example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			dataStream = env.fromElements(WORDS);
		}

		DataStream<Tuple2<String, Integer>> outputStream = dataStream.map(new ExtractPopulation())
																	 .keyBy(0)
																	 .min(1);

		// emit result
		if (params.has("output")) {
			outputStream.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			outputStream.print();
		}

		// execute program
		env.execute("Streaming ExampleNumAgg");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	
	public static class ExtractPopulation implements MapFunction<String, Tuple2<String, Integer>> {
		@Override
		public Tuple2<String, Integer> map(String data) throws Exception {
			String[] fields = data.split("\t");
			return new Tuple2<String, Integer>(
					fields[0].trim(), /* continent */
					Integer.parseInt(fields[1].split(",")[1].trim())); /* population */
		}
	}
	

}
