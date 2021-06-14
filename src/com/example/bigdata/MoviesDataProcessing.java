package com.example.bigdata;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MoviesDataProcessing {

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        //config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "...:...");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "movies-data-etl");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> intSerde = Serdes.Integer();
        final Serde<MovieRateRecord> movieRateSerde = Serdes.serdeFrom(new MovieRateRecordSerializer(), new MovieRateRecordDeserializer());
        final Serde<MovieAggregate> movieAggregateSerde = Serdes.serdeFrom(new MovieAggregateSerializer(), new MovieAggregateDeserializer());

        final StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> movieTitles = builder
                .stream("movie-titles", Consumed.with(stringSerde, stringSerde))
                .map((key, line) -> {
                    String[] splittedLine = line.split(",");
                    return KeyValue.pair(splittedLine[0], splittedLine[2]);
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue, Materialized.as("movieTitles"));

        movieTitles.toStream().to("movieTitles");

        KStream<String, MovieRateRecord> movieRatesStream = builder
                .stream("movie-rates", Consumed.with(stringSerde, stringSerde))
                .filter((key, line) -> MovieRateRecord.lineIsCorrect(line))
                .map((key, line) ->  {
                    MovieRateRecord record = MovieRateRecord.parseFromLine(line);
                    return KeyValue.pair(record.getFilmId(), record);
                })
                .join(movieTitles, (movieRecord, title) -> {
                    movieRecord.setTitle(title);
                    return movieRecord;
                }, Joined.with(stringSerde, movieRateSerde, stringSerde));

        KTable<String, MovieAggregate> movieAggregates = movieRatesStream
                .map((key, value) -> KeyValue.pair(value.getFilmId() + ";" + value.getTitle() + ";" + value.getMonth(), value))
                .groupByKey(Grouped.with(stringSerde, movieRateSerde))
                .aggregate(MovieAggregate::new,
                        (key, value, aggregated) -> {
                            aggregated.update(value);
                            return aggregated;
                        },
                        Materialized.with(stringSerde, movieAggregateSerde));

        movieAggregates.toStream().to("movie-aggregates");

        final Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, config);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

    static class MovieRateRecordSerializer implements Serializer<MovieRateRecord> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, MovieRateRecord movieRateRecord) {
            StringSerializer stringSerializer = new StringSerializer();
            return stringSerializer.serialize(topic, movieRateRecord.toString());
        }

        @Override
        public void close() {

        }
    }

    static class MovieRateRecordDeserializer implements Deserializer<MovieRateRecord> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public MovieRateRecord deserialize(String topic, byte[] bytes) {
            StringDeserializer stringDeserializer = new StringDeserializer();
            String movieRateRecord = stringDeserializer.deserialize(topic, bytes);
            return MovieRateRecord.fromString(movieRateRecord);
        }

        @Override
        public void close() {

        }
    }

    static class MovieAggregateSerializer implements Serializer<MovieAggregate> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, MovieAggregate movieAggregate) {
            StringSerializer stringSerializer = new StringSerializer();
            return stringSerializer.serialize(topic, movieAggregate.toString());
        }

        @Override
        public void close() {

        }
    }

    static class MovieAggregateDeserializer implements Deserializer<MovieAggregate> {

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public MovieAggregate deserialize(String topic, byte[] bytes) {
            StringDeserializer stringDeserializer = new StringDeserializer();
            String movieAggregateString = stringDeserializer.deserialize(topic, bytes);
            return MovieAggregate.fromString(movieAggregateString );
        }

        @Override
        public void close() {

        }
    }


}