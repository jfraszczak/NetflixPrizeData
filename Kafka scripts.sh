KAFKA CLUSTER
----------------------

gcloud beta dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --bucket ${BUCKET_NAME} \
--region ${REGION} --subnet default --zone ${ZONE} \
--master-machine-type n1-standard-2 --master-boot-disk-size 50 \
--num-workers 2 \
--worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.0-debian10 \
--optional-components ZEPPELIN,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=10h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh


PRODUCER
----------------------

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

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer movie_rates 1 movie-rates 1 ${CLUSTER_NAME}-w-0:9092
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer movie_titles 0 movie-titles 1 ${CLUSTER_NAME}-w-0:9092


CONSUMER
----------------------

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic aggregated-movies --from-beginning
kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic anomalous-movies --from-beginning


DATA PROCESSING
----------------------

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:NetflixPrizeData.jar com.example.bigdata.MoviesDataProcessing ${CLUSTER_NAME}-w-0:9092 200 3 30