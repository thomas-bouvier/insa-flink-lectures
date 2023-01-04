package io.thomas;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleCoMap" -Dexec.args="--input1 countries-stream.txt --input2 countries-stream-v2.txt" -q
 */
public class ExampleCoMap {
	

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
		
		DataStream<Tuple2<String, String>> coMapStream = stream1.connect(stream2).map(new ExtractCountries());

		// emit result
		if (params.has("output")) {
			coMapStream.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			coMapStream.print();
		}

		// execute program
		env.execute("Streaming ExampleCoMap");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class ExtractCountries implements CoMapFunction<String, String, Tuple2<String, String>> {
		@Override
		public Tuple2<String, String> map1(String value) throws Exception {
			String[] data = value.split("\t");
			return Tuple2.of(data[0].trim(), data[1].trim());
		}
		@Override
		public Tuple2<String, String> map2(String value) throws Exception {
			String[] data = value.split("\t");
			return Tuple2.of(data[0].trim(), data[1].trim());
		}
	}
	
}
