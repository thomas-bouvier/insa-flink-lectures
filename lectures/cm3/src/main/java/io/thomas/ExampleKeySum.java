package io.thomas;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleKeySum" -q
 */
public class ExampleKeySum {
    
    public static final String[] WORDS = new String[] {
            "Asia    Japan, Japan, Malaysia, Nepal, Singapore",
            "Europe    Austria, Belgium, France, Greece, Spain, Romania",
            "North America    Canada, Cuba, US, Mexico",
            "South America    Argentina, Brazil, Chile, Venezuela, Uruguay",
            "Oceania    Australia, Fiji, New Zealand, Tonga",
            "Africa    Angola, Egypt, Kenya, Morocco"
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
            System.out.println("Executing KeySum example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            dataStream = env.fromElements(WORDS);
        }

        DataStream<Tuple2<String,Integer>> outputStream = dataStream.map(new ExtractCountries())
                                                                   .flatMap(new SplitCountries())
                                                                   .keyBy(0)
                                                                   .sum(1);

        // emit result
        if (params.has("output")) {
            outputStream.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            outputStream.print();
        }

        // execute program
        env.execute("Streaming KeySumExample");
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
    
    public static class SplitCountries implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] countries = input.split(",");
            for (String country : countries) {
                out.collect(new Tuple2<String, Integer>(country.trim(), 1));
            }
        }
    }

}
