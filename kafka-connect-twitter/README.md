# Kafka Twitter Connector

## Usage

### Download [kafka-connect-twitter](https://github.com/jcustenborder/kafka-connect-twitter)

Run in this folder:
```bash
wget https://github.com/jcustenborder/kafka-connect-twitter/releases/download/0.2.26/kafka-connect-twitter-0.2.26.tar.gz -O connector.tar.gz
tar xvf connector.tar.gz -C connectors
```

### Configure Twitter

Copy `twitter.properties.example` to `twitter.properties` and add your Twitter keys.

### Create Kafka topics

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter_status_connect --partitions 3 --replication-factor 1
$ kafka-topics.sh --bootstrap-server localhost:9092 --create --topic twitter_delete_connect --partitions 3 --replication-factor 1
```

List topics:

```bash
$ kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Start Twitter source connector

```bash
$ ./run.sh
```

or 

```bash
$ connect-standalone.sh connect-standalone.properties twitter.properties
```

Start a console consumer with [kafkacat](https://github.com/edenhill/kafkacat):

```bash
$ kafkacat -b localhost:9092 -t twitter_status_connect -C
``` 
