# Kafka-Twitter-ElasticSearch demo

Example of streaming data from Twitter to ElasticSearch via Kafka from Stephane Maarek's [Learn Apache Kafka for Beginners](https://www.udemy.com/course/apache-kafka/) course.

I've added multiproject build with Gradle.

## Instructions

### Kafka

1. Start Zookeeper: `zookeeper-server-start.sh /path/to/config/zookeeper.properties`
1. Start Kafka: `kafka-server-start.sh /path/to/config/server.properties`

### Twitter producer

1. Add Twitter credentials in `twitter-producer/gradle.properties` by using `twitter-producer/gradle.properties.example` as template.
1. Start the Twitter producer: `./gradlew twitter-producer:run`

### ElasticSearch consumer

#### Setting up the cluster with Docker

First you need to setup an ElasticSearch cluster. If you have Docker, follow the instructions [here](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/docker.html):

```bash
$ docker pull docker.elastic.co/elasticsearch/elasticsearch:6.8.6
$ docker run --rm --name elasticsearch -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.6
```

If you want to add a custom configuration, add a bind mount:

```bash
$ docker run ... -v `pwd`/custom_elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:ro
```

You can also setup Kibana with Docker:

```bash
$ docker pull docker.elastic.co/kibana/kibana:6.8.6
$ docker run --link elasticsearch:elasticsearch -p 5601:5601 docker.elastic.co/kibana/kibana:6.8.6
```

#### Starting the consumer

```bash
$ ./gradlew elasticsearch-consumer:run
```

## Debugging

Consume tweets from `twitter_tweets` topic to stdout with `kafkacat`:

```bash
$ kafkacat -b localhost:9092 -t twitter_tweets
```
