package io.thomas;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * mvn install exec:java -Dmain.class="io.thomas.producers.DataProducer" -q
 * mvn install exec:java -Dmain.class="io.thomas.ExampleState" -q
 */
public class ExampleState {
    
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // starts a checkpoint every 2 seconds
        env.enableCheckpointing(2000);
        // sets the minimal pause between checkpointing attempts to 10 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // sets the maximum number of checkpoint attempts that may be in progress at the same time to one
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // sets the checkpointing mode
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // set checkpoint storage
        env.getCheckpointConfig().setCheckpointStorage("file:///home/tbouvier/example-state-store");

        env.setStateBackend(new HashMapStateBackend());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                2, // number of restart attempts
                3000 // delay in-between restart attempts
        ));
        
        
        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
        
        DataStream<Tuple4<Integer, Long, Double, Double>> outputStream = dataStream.map(new FormatData())
                                                                                      .keyBy(t -> t.f0)
                                                                                      .flatMap(new SaveState());
        outputStream.print();
        // emit result
        outputStream.sinkTo(FileSink.<Tuple4<Integer, Long, Double, Double>>forRowFormat(
                new Path("lectures/cm4/state"),
                new SimpleStringEncoder<>()
        ).build());

        // execute program
        env.execute("Streaming ExampleState");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    
    public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) {
            return Tuple2.of(0,
                             Double.parseDouble(value.split(" ")[2].trim()));
        }
    }

    public static class SaveState extends RichFlatMapFunction<Tuple2<Integer, Double>, Tuple4<Integer, Long, Double, Double>> {

        private transient ValueState<Long> count;
        private transient MapState<Integer, Double> measurements;
        
        @Override
        public void flatMap(Tuple2<Integer, Double> value, Collector<Tuple4<Integer, Long, Double, Double>> out) throws Exception {
            int sensorId = value.f0;
            double sensorMeasurement = value.f1;
            
            Long currCount = count.value();
            currCount += 1;
            count.update(currCount);

            if (!measurements.contains(sensorId)) {
                measurements.put(sensorId, sensorMeasurement);
            } else {
                double cumulativeMeasurements = measurements.get(sensorId);
                measurements.put(sensorId, cumulativeMeasurements + sensorMeasurement);
            }
            
            if (currCount >= 10) {
                /* emit last 10 measurements */
                out.collect(Tuple4.of(sensorId, currCount, measurements.get(sensorId), measurements.get(sensorId) / currCount));
                /* clear value */
                count.clear();
                measurements.clear();
            }
        }
        
        @Override
        public void open(Configuration conf)
        {
            MapStateDescriptor<Integer, Double> descriptor = new MapStateDescriptor<>(
                    "measurements",
                    TypeInformation.of(new TypeHint<>() {}),
                    TypeInformation.of(new TypeHint<>() {})
            );
            measurements = getRuntimeContext().getMapState(descriptor);
            
            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>(
                    "count",
                    TypeInformation.of(new TypeHint<>() {}),
                    0L
            );
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
