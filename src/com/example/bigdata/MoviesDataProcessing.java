package com.example.bigdata;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;
import java.time.LocalDate;

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

        int MINIMAL_RATES_NUMBER = Integer.parseInt(args[1]);
        float MINIMAL_RATES_AVERAGE = Integer.parseInt(args[2]);
        int WINDOW_LENGTH = Integer.parseInt(args[3]);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> intSerde = Serdes.Integer();
        final Serde<MovieRateRecord> movieRateSerde = Serdes.serdeFrom(new MovieRateRecordSerializer(), new MovieRateRecordDeserializer());
        final Serde<MovieAggregate> movieAggregateSerde = Serdes.serdeFrom(new MovieAggregateSerializer(), new MovieAggregateDeserializer());

        final StreamsBuilder builder = new StreamsBuilder();


        // Add titles
        KTable<String, String> movieTitles = builder
                .stream("movie-titles", Consumed.with(stringSerde, stringSerde))
                .map((key, line) -> {
                    String[] splittedLine = line.split(",");
                    return KeyValue.pair(splittedLine[0], splittedLine[2]);
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> newValue, Materialized.as("movieTitles"));

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


        // Aggregation
        KTable<String, MovieAggregate> movieAggregates = movieRatesStream
                .map((key, value) -> KeyValue.pair(value.getFilmId() + "-" + value.getTitle() + "-" + value.getMonth(), value))
                .groupByKey(Grouped.with(stringSerde, movieRateSerde))
                .aggregate(MovieAggregate::new,
                        (key, value, aggregated) -> {
                            aggregated.updateRates(value);
                            return aggregated;
                        },
                        Materialized.with(stringSerde, movieAggregateSerde));

        KTable<String, String> movieUniqueUsers = movieRatesStream
                .map((key, value) -> KeyValue.pair(value.getFilmId() + "-" + value.getTitle() + "-" + value.getMonth() + "-" + value.getUserId(), value.getFilmId() + "-" + value.getTitle() + "-" + value.getMonth()))
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .reduce((aggValue, newValue) -> newValue, Materialized.as("movie_users"));

        KTable<String, MovieAggregate> movieUniqueUsersCounts = movieUniqueUsers
                .toStream()
                .map((key, value) -> KeyValue.pair(value, value))
                .groupByKey(Grouped.with(stringSerde, stringSerde))
                .aggregate(MovieAggregate::new,
                        (key, value, aggregated) -> {
                            aggregated.incrementUniqueRatesCount();
                            return aggregated;
                        },
                        Materialized.with(stringSerde, movieAggregateSerde));

        KStream<String, MovieAggregate> finalMovieAggregates = movieAggregates
                .toStream()
                .join(movieUniqueUsersCounts, (movieAggregate, uniqueUsers) -> {
                    movieAggregate.setUniqueRatesCount(uniqueUsers.getUniqueRatesCount());
                    return movieAggregate;
                }, Joined.with(stringSerde, movieAggregateSerde, movieAggregateSerde));


        // Anomaly detection
        KStream<String, String> anomalousMovies  = movieRatesStream
                .map((key, value) -> KeyValue.pair(value.getFilmId() + ";" + value.getTitle(), value))
                .groupByKey(Grouped.with(stringSerde, movieRateSerde))
                .windowedBy(TimeWindows.of(Duration.ofDays(WINDOW_LENGTH)).advanceBy(Duration.ofDays(1)))
                .aggregate(MovieAggregate::new,
                        (key, value, aggregated) -> {
                            aggregated.updateRates(value);
                            return aggregated;
                        },
                        Materialized.with(stringSerde, movieAggregateSerde))

                .toStream()
                .filter((key, value) -> value.anomalyDetected(MINIMAL_RATES_NUMBER, MINIMAL_RATES_AVERAGE))
                .map((key, value) -> {
                    String startDate = timestampToDate(key.window().start());
                    String endDate = timestampToDate(key.window().end());
                    return KeyValue.pair(key.key() + ";" + startDate + ";" + endDate,
                            anomalyToString(key.key(), value, startDate, endDate));
                });


        finalMovieAggregates
                .map((key, aggregate) -> KeyValue.pair(key, aggregateToString(key, aggregate)))
                .to("aggregated-movies");

        anomalousMovies
                .map(KeyValue::pair)
                .to("anomalous-movies");

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

    private static String aggregateToString(String key, MovieAggregate aggregate){
        return key + ';' + aggregate.getRateCount() + ';' + aggregate.getRateSum() + ';' + aggregate.getUniqueRatesCount();
    }

    private static String anomalyToString(String key, MovieAggregate aggregate, String startDate, String endDate){
        return startDate + "-" + endDate + ";" + key + ';' + aggregate.getRateCount() + ';' + aggregate.getRatesAverage();
    }

    private static String timestampToDate(long timestamp) {
        LocalDate date = LocalDate.ofEpochDay((timestamp/86400000) - 3653);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return date.format(formatter);
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