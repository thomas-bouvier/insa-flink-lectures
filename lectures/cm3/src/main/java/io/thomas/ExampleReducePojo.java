package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.thomas.utils.ContinentAvg;
import io.thomas.utils.ContinentStats;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleReducePojo" -q
 */
public class ExampleReducePojo {
    
    public static final String[] WORDS = new String[] {
            "Europe;Belgium, 11539878, 0.147",
            "Europe;France, 67146000, 0.858",
            "Europe;Greece, 10724599, 0.137",
            "Europe;Spain, 47329981, 0.605",
            "Europe;Austria, 8935112, 0.114",
            "Europe;Romania, 19317984, 0.247",
            "Asia;China, 1405384280, 8.0",
            "Asia;Japan, 125880000, 1.61",
            "Asia;Malaysia, 32709470, 0.418",
            "Europe;Spain, 47329981, 0.605",
            "America;United States, 330674288, 4.22",
            "America;Brazil, 212356116, 2.71"
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
            System.out.println("Use --input to specify file input.");
            // get default test text data
            dataStream = env.fromElements(WORDS);
        }

        DataStream<ContinentAvg> outputStream = dataStream.map(new ExtractPopulation())
                                                          .keyBy(c -> c.continent)
                                                          .reduce(new SumCountPopulation())
                                                          .map(new AvgPopulation());

        // emit result
        if (params.has("output")) {
            outputStream.sinkTo(FileSink.<ContinentAvg>forRowFormat(new Path(params.get("output")), new SimpleStringEncoder<>()).build());
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            outputStream.print();
        }

        // execute program
        env.execute("Streaming ExampleReducePojo");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    
    public static class ExtractPopulation implements MapFunction<String, ContinentStats> {
        @Override
        public ContinentStats map(String data) {
            String[] fields = data.split(";");
            return new ContinentStats(
                    fields[0].trim(), /* continent */
                    Double.parseDouble(fields[1].split(",")[1].trim()), /* population */
                    1 /* count */); 
        }
    }
    
    public static class SumCountPopulation implements ReduceFunction<ContinentStats> {
        @Override
        public ContinentStats reduce(ContinentStats mycumulative,
                                     ContinentStats input) {
            return new ContinentStats(
                        input.continent,
                        mycumulative.population + input.population,
                        mycumulative.count + 1);
        }
    }
    
    public static class AvgPopulation implements MapFunction<ContinentStats, ContinentAvg> {
        @Override
        public ContinentAvg map(ContinentStats value) {
            return new ContinentAvg(
                    value.continent,
                    value.population / value.count);
        }
    }
    

}
