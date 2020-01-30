# Kafka-Twitter-ElasticSearch demo

Example of streaming data from Twitter to ElasticSearch via Kafka from Stephane Maarek's [Learn Apache Kafka for Beginners](https://www.udemy.com/course/apache-kafka/) course.

Stuff I (@ksaaskil) have added:

- `gradle` instead of `mvn`
- Reading Twitter credentials from `gradle.properties`
- `slf4j-simplelogger` and a configuration file for it
- Instructions for running ElasticSearch via Docker Compose

## Instructions

### Starting Kafka and Zookeeper

To start both Kafka and Zookeeper, modify `KAFKA_DIR` in [start-zookeeper-kafka.sh](./start-zookeeper-kafka.sh) and run:

```bash
$ ./start-zookeeper-kafka.sh
# To stop:
$ ./stop-zookeeper-kafka.sh
```

Logs are written to `kafka.log` and `zookeeper.log`.

### Starting Twitter producer

1. Add Twitter credentials in `twitter-producer/gradle.properties` by using `twitter-producer/gradle.properties.example` as template.
1. Start the Twitter producer: `./gradlew twitter-producer:run`

### Setting up ElasticSearch

#### Using docker-compose

Use [docker-compose.yml](./docker-compose.yml):

```bash
$ docker-compose up -d  # Start ES and Kibana
$ docker-compose down  # Stop cluster
```

Now you can explore [http://localhost:5601](http://localhost:5601).

#### Using Docker

Use the bash scripts included in the repository:

```bash
$ ./start-es.sh
$ ./stop-es.sh
```

More explicitly:

```bash
$ docker run --rm --name elasticsearch -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.6
```

If you want to add a custom configuration, add a bind mount:

```bash
$ docker run ... -v `pwd`/custom_elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:ro
```

Setup Kibana:

```bash
$ docker run --rm --name kibana --link elasticsearch -p 127.0.0.1:5601:5601 -v `pwd`/kibana.yml:/usr/share/kibana/config/kibana.yml docker.elastic.co/kibana/kibana:6.8.6
```

### Starting ElasticSearch consumer

```bash
$ ./gradlew elasticsearch-consumer:run
```

## Debugging

Consume tweets from `twitter_tweets` topic to stdout with `kafkacat`:

```bash
$ kafkacat -b localhost:9092 -t twitter_tweets
```
