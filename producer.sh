CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic movie-rates
/usr/lib/kafka/bin/kafka-topics.sh --create --zookeeper ${CLUSTER_NAME}-m:2181 --replication-factor 1 --partitions 1 --topic movie-titles

kafka-topics.sh --zookeeper localhost:2181 --list

mkdir movie_titles
cd movie_titles
hadoop fs -copyToLocal gs://big-data-put/project/movie_titles.csv
cd ..

mkdir movie_rates
cd movie_rates
hadoop fs -copyToLocal gs://big-data-put/project/netflix-prize-data/*
cd ..

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer movie_titles 0 movie-titles 1 ${CLUSTER_NAME}-w-0:9092
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer movie_rates 1 movie-rates 1 ${CLUSTER_NAME}-w-0:9092