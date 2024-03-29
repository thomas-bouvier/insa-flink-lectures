package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleFlatMap" -q
 * mvn install exec:java -Dmain.class="io.thomas.ExampleFlatMap" -Dexec.args="--input datasets/countries-stream.txt" -q
 */
public class ExampleFlatMap {

    public static final String[] WORDS = new String[] {
            "Asia;China, Japan, Malaysia, Nepal, Singapore",
            "Europe;Austria, Belgium, France, Greece, Spain, Romania",
            "North America;Canada, Cuba, US, Mexico",
            "South America;Argentina, Brazil, Chile, Venezuela, Uruguay",
            "Oceania;Australia, Fiji, New Zealand, Tonga",
            "Africa;Angola, Egypt, Kenya, Morocco"
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
            final FileSource<String> source =
                    FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(params.get("input"))).build();

            // read the text file from given input path
            dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");
        } else {
            System.out.println("Executing FlatMap example with default input data set.");
            System.out.println("Use --input to specify file input.");
            // get default test text data
            dataStream = env.fromElements(WORDS);
        }

        DataStream<String> outputStream = dataStream.map(new ExtractCountries())
                                                    .flatMap(new SplitCountries());

        // emit result
        if (params.has("output")) {
            outputStream.sinkTo(FileSink.<String>forRowFormat(new Path(params.get("output")), new SimpleStringEncoder<>()).build());
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            outputStream.print();
        }

        // execute program
        env.execute("ExampleFlatMap");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    
    public static class ExtractCountries implements MapFunction<String, String> {
        @Override
        public String map(String continent) {
            return continent.split(";")[1];
        }
    }
    
    public static class SplitCountries implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String input, Collector<String> out) {
            String[] countries = input.split(",");
            for (String country : countries) {
                out.collect(country.trim());
            }
        }
    }
}
