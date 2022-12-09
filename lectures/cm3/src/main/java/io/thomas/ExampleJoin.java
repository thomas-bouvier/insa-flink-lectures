package io.thomas;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class ExampleJoin {
	

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
		
		
		DataSet<Tuple2<Integer, String>> countriesSet = dataset1.map(new FormatDataCountry());

		DataSet<Tuple2<Integer, String>> peopleSet = dataset2.map(new FormatDataPeople());
		
		DataSet<Tuple3<Integer, String, String>> joinedData = countriesSet.join(peopleSet)
																		  .where(0)
																		  .equalTo(0)
																		  .with(new JoinData());

		
		// emit result
		if (params.has("output")) {
			joinedData.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			joinedData.print();
		}

		// execute program
		env.execute("Streaming WordCount");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class FormatDataCountry implements MapFunction<String, Tuple2<Integer, String>> {
		@Override
		public Tuple2<Integer, String> map(String data) throws Exception {
			return new Tuple2<Integer, String>(
						Integer.parseInt(data.split(",")[0]),
						data.split(",")[1].trim());
		}
	}
	
	public static class FormatDataPeople implements MapFunction<String, Tuple2<Integer, String>> {
		@Override
		public Tuple2<Integer, String> map(String data) throws Exception {
			return new Tuple2<Integer, String>(
						Integer.parseInt(data.split(",")[2].trim()),
						data.split(",")[1].trim());
		}
	}
	
	public static class JoinData implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
		@Override
		public Tuple3<Integer, String, String> join(Tuple2<Integer, String> countries, Tuple2<Integer, String> person) throws Exception {
			return new Tuple3<Integer, String, String>(
					countries.f0,
					person.f1,
					countries.f1);
		}
	}
	

}
