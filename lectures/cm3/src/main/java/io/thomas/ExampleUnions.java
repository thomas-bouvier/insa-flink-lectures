package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleUnions" -Dexec.args="--input1 datasets/countries-stream.txt --input2 datasets/countries-stream-v2.txt" -q
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
        final FileSource<String> source1 =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(params.get("input1"))).build();
        final FileSource<String> source2 =
                FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(params.get("input2"))).build();

        DataStream<String> stream1 = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "file-source-1");
        DataStream<String> stream2 = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "file-source-2");
        
        DataStream<String> countriesUnion = stream1.union(stream2).map(new ExtractCountries())
                                                                     .flatMap(new SplitCountries());

        
        // emit result
        if (params.has("output")) {
            countriesUnion.sinkTo(FileSink.<String>forRowFormat(new Path(params.get("output")), new SimpleStringEncoder<>()).build());
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
