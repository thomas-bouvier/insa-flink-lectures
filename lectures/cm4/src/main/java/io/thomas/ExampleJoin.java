package io.thomas;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * mvn install exec:java -Dmain.class="io.thomas.ExampleJoin" -Dexec.args="--input1 country-ids-stream.txt --input2 people-ids-stream.txt" -q
 */
public class ExampleJoin {

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
        
        
        DataStream<Tuple2<Integer, String>> countriesStream = stream1.map(new FormatDataCountry());
        DataStream<Tuple2<Integer, String>> peopleStream = stream2.map(new FormatDataPeople());

        DataStream<Tuple3<Integer, String, String>> joinedData = countriesStream.join(peopleStream)
                                                                              .where(t -> t.f0)
                                                                              .equalTo(t -> t.f0)
                                                                              .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                                                                              .apply(new JoinData());

        /*
        DataStream<Tuple3<Integer, String, String>> joinedData = countriesStream.join(peopleStream)
                                                                              .where(new KeySelector<Tuple2<Integer, String>, Integer>() {
                                                                                @Override
                                                                                public Integer getKey(Tuple2<Integer, String> country) throws Exception {
                                                                                    System.out.println("country " + country.f0);
                                                                                    return country.f0;
                                                                                }
                                                                            })
                                                                              .equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
                                                                                @Override
                                                                                public Integer getKey(Tuple2<Integer, String> person) throws Exception {
                                                                                    System.out.println("person " + person.f0);
                                                                                    return person.f0;
                                                                                }
                                                                            })
                                                                              .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                                                                              .apply(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                                                                                @Override
                                                                                public Tuple3<Integer, String, String> join(Tuple2<Integer, String> countries, Tuple2<Integer, String> person) throws Exception {
                                                                                    System.out.println(countries.f0 + " " + person.f1 + " " + countries.f1);
                                                                                    return new Tuple3<>(countries.f0, person.f1, countries.f1);
                                                                                }
                                                                            });
        */

        // emit result
        if (params.has("output")) {
            joinedData.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            joinedData.print();
        }

        // execute program
        env.execute("Streaming ExampleJoin");
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
            System.out.println(countries.f0 + " " + person.f1 + " " + countries.f1);
            return new Tuple3<>(countries.f0, person.f1, countries.f1);
        }
    }

}
