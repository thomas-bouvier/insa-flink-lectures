package io.thomas;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class ExampleState {
    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        
        // starts a checkpoint every 2 seconds
        env.enableCheckpointing(2000);
        // sets the minimal pause between checkpointing attempts to 10 seconds
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
        // sets the maximum number of checkpoint attempts that may be in progress at the same time to one
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // sets the checkpointing mode
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        env.setStateBackend((StateBackend) new FsStateBackend("file:///home/student/example-state-store"));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                2, // number of restart attempts
                3000 // delay in-between restart attempts
        ));
        
        
        DataStream<String> dataStream = env.socketTextStream("localhost", 9090);
        
        DataStream<Tuple4<Integer, Long, Double, Double>> outputStream = dataStream.map(new FormatData())
                                                                                      .keyBy(0)
                                                                                      .flatMap(new SaveState());

        // emit result
        outputStream.writeAsText("example-state.txt", WriteMode.OVERWRITE);
        // execute program
        env.execute("Streaming ExampleState");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************
    
    public static class FormatData implements MapFunction<String, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(String value) throws Exception {
            return Tuple2.of(Integer.parseInt(value.split("  ")[0].trim()), 
                             Double.parseDouble(value.split("  ")[1].trim()));
        }
    }

    public static class SaveState extends RichFlatMapFunction<Tuple2<Integer, Double>, Tuple4<Integer, Long, Double, Double>> {

        private transient ValueState<Long> count;
        private transient ValueState<Map<Integer, Double>> allMeasurements;
        
        @Override
        public void flatMap(Tuple2<Integer, Double> value, Collector<Tuple4<Integer, Long, Double, Double>> out) throws Exception {
            int sensorId = value.f0;
            double sensorMeasurement = value.f1;
            
            Long currCount = count.value();
            currCount += 1;
            count.update(currCount);
            
            Map<Integer, Double> savedMeasurements = allMeasurements.value();
            if (!savedMeasurements.containsKey(sensorId)) {
                savedMeasurements.put(sensorId, sensorMeasurement);
            } else {
                double cumulativeMeasurements = savedMeasurements.get(sensorId);
                savedMeasurements.put(sensorId, cumulativeMeasurements + sensorMeasurement);
            }
            allMeasurements.update(savedMeasurements);
            
            if (currCount >= 10) {
                /* emit last 10 measurements */
                out.collect(Tuple4.of(sensorId, currCount, savedMeasurements.get(sensorId), savedMeasurements.get(sensorId)/currCount));
                /* clear value */
                count.clear();
                allMeasurements.clear();
            }
        }
        
        @SuppressWarnings("deprecation")
        @Override
        public void open(Configuration conf)
        {
            ValueStateDescriptor<Map<Integer, Double>> descriptor = 
                    new ValueStateDescriptor<Map<Integer, Double>>(
                            "allMeasurements",
                            TypeInformation.of(new TypeHint<Map<Integer, Double>>(){}), 
                            new HashMap<Integer, Double>());
            allMeasurements = getRuntimeContext().getState(descriptor);
            
            ValueStateDescriptor<Long> descriptor2 = 
                    new ValueStateDescriptor<Long>(
                            "count",
                            TypeInformation.of(new TypeHint<Long>(){}), 
                            0L);
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
