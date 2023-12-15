package io.thomas;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.math.BigInteger;

class Event {
    public String id;
    public String metadata;

    public Event(String id, String metadata) {
        this.id = id;
        this.metadata = metadata;
    }
}

class Statistics {
    public int count;

    public Statistics(int count) {
        this.count = count;
    }
}

class MyRecordSerializationSchema implements KafkaRecordSerializationSchema<Statistics> {

    private String topic;

    public MyRecordSerializationSchema(String topic) {
        this.topic = topic;
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Statistics statistics, KafkaSinkContext kafkaSinkContext, Long aLong) {
        return new ProducerRecord<>(topic, BigInteger.valueOf(statistics.count).toByteArray());
    }
}

public class ExampleKafka {

    final static String inputTopic = "input-topic";
    final static String outputTopic = "output-topic";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "localhost:9092";

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // get input data
        DataStream<String> dataStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        DataStream<Event> events = dataStream.map(new ParseEvent());

        DataStream<Statistics> stats = events
                .keyBy(e -> e.id)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new MyWindowAggregationFunction());

        // emit result
        KafkaSink<Statistics> sink = KafkaSink.<Statistics>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new MyRecordSerializationSchema(outputTopic))
                .build();
        stats.sinkTo(sink);

        // execute program
        env.execute("Streaming ExampleKafka");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class ParseEvent implements MapFunction<String, Event> {
        @Override
        public Event map(String data) {
            String id = data.split(";")[0];
            String metadata = data.split(";")[1];
            return new Event(id, metadata);
        }
    }

    public static class MyWindowAggregationFunction implements AggregateFunction<Event, Statistics, Statistics> {

        @Override
        public Statistics createAccumulator() {
            return null;
        }

        @Override
        public Statistics add(Event event, Statistics statistics) {
            return null;
        }

        @Override
        public Statistics getResult(Statistics statistics) {
            return null;
        }

        @Override
        public Statistics merge(Statistics statistics, Statistics acc1) {
            return null;
        }
    }
}
