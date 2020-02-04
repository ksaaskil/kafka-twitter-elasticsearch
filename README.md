# Kafka-Twitter-ElasticSearch demo

Example of streaming data from Twitter to ElasticSearch via Kafka.

Based on Stephane Maarek's [Learn Apache Kafka for Beginners](https://www.udemy.com/course/apache-kafka/) course.

## Stuff I have added (@ksaaskil)

- `gradle` instead of `mvn`
- Reading Twitter credentials from `gradle.properties`
- `slf4j-simplelogger` and a configuration file for it in `resources/`
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

### Starting Twitter producer ([twitter-producer](./twitter-producer))

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

#### Exploring ElasticSearch

List indices:

```bash
$ curl "localhost:9200/_cat/indices?v"
```

Create an index `twitter`:

```bash
$ curl -X PUT "localhost:9200/twitter"
```

Add a tweet:

```bash
$ curl -X PUT "localhost:9200/twitter/tweets/1" -H 'Content-Type: application/json' -d '{ "course": "Kafka" }'
```

### Starting ElasticSearch consumer ([elasticsearch-consumer](./elasticsearch-consumer))

```bash
$ ./gradlew elasticsearch-consumer:run
```

## Debugging

Consume tweets from `twitter_tweets` topic to stdout with `kafkacat`:

```bash
$ kafkacat -b localhost:9092 -t twitter_tweets
```

Check consumer group offsets:

```bash
$ kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group es-consumer-1 --describe
```


## Using the Twitter connector

See [kafka-connect-twitter](./kafka-connect-twitter).

## Using the Kafka Stream ([stream-tweets](./stream-tweets))

Create topic `twitter_tweets_important`:

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter_tweets_important --partitions 3 --replication-factor 1
```

Start the stream:

```bash
$ ./gradlew stream-tweets:run
```

Consume filtered tweets to console:

```bash
$ kafkacat -b localhost:9092 -t twitter_tweets_important -C
```
