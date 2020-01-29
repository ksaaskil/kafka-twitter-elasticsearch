# Kafka-Twitter-ElasticSearch demo

Example of streaming data from Twitter to ElasticSearch via Kafka from Stephane Maarek's [Learn Apache Kafka for Beginners](https://www.udemy.com/course/apache-kafka/) course.

I've added multiproject build with Gradle.

## Instructions

1. Start Zookeeper: `zookeeper-server-start.sh /path/to/config/zookeeper.properties`
1. Start Kafka: `kafka-server-start.sh /path/to/config/server.properties`
1. Add Twitter credentials in `twitter-producer/gradle.properties` by using `twitter-producer/gradle.properties.example` as template.
1. Start the Twitter producer: `./gradlew twitter-producer:run`
1. TODO...