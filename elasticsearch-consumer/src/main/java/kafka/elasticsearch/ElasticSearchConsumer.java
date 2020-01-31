package kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    public static String KAFKA_TOPIC = "twitter_tweets";
    public static String GROUP_ID = "es-consumer-1";
    private final RestHighLevelClient esClient;
    private final KafkaConsumer<String, String> kafkaConsumer;

    public ElasticSearchConsumer(RestHighLevelClient esClient, KafkaConsumer<String, String> kafkaConsumer) {
        this.esClient = esClient;
        this.kafkaConsumer = kafkaConsumer;
    }

    protected void start() {
        LOG.info("Creating consumer thread");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(this.esClient, this.kafkaConsumer);
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future future = executor.submit(consumerRunnable);

        Thread shutdownHook = new Thread(() -> {
            consumerRunnable.shutdown();

            try {
                executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
                LOG.info("Exiting cleanly");
            } catch (InterruptedException e) {
                LOG.error("Did not exit cleanly", e);
            }
        });

        LOG.info("Adding shutdown hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    protected static void run() {
        RestHighLevelClient esClient = createEsClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer(esClient, consumer);

        elasticSearchConsumer.start();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = createProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(KAFKA_TOPIC));
        return consumer;
    }

    private static RestHighLevelClient createEsClient() {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );
        return new RestHighLevelClient(builder);
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        // Base properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest,
//
//        // Safe producer properties
//        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
//        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
//        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
//        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
//
//        // High-throughput producer properties
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // lz4, gzip, etc.
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "65536");  // 64 kB
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");  // Linger a while for larger batches

        return properties;
    }

    public static String readEnv(String env) {
        String value = System.getenv(env);
        if (value == null) {
            LOG.info("Missing environment variable {}", env);
        } else {
            LOG.info("Environment variable loaded: {}", env);
        }
        return value == null ? "N/A" : value;
    }

    public static class ConsumerRunnable implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final RestHighLevelClient esClient;
        // private static JsonParser jsonParser = new JsonParser();

        private ConsumerRunnable(RestHighLevelClient esClient, KafkaConsumer<String, String> consumer) {
            this.esClient = esClient;
            this.consumer = consumer;
        }

        private static JsonObject parseAsJsonObject(String obj) {
            return JsonParser.parseString(obj).getAsJsonObject();
        }

        private static String extractIdFromTweet(String jsonString) {
            return parseAsJsonObject(jsonString).get("id_str").getAsString();
        }

        protected void index(ConsumerRecord<String, String> record) {

            // Kafka generic ID
            // String id = record.topic() + record.partition() + record.offset();

            String tweetRecord = record.value();

            JsonObject parsedTweetRecord = parseAsJsonObject(tweetRecord);

            String text = parsedTweetRecord.get("text").getAsString();

            // Twitter ID
            String id = extractIdFromTweet(tweetRecord);

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("id", id);
            jsonMap.put("text", text);

            IndexRequest indexRequest = new IndexRequest("twitter")
                    .id(id)
                    .source(jsonMap);

            ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    LOG.info("Document indexed with id {}", indexResponse.getId());
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Failed indexing message to ElasticSearch", e);
                }
            };

            this.esClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOG.debug("Key: " + record.key() + ", Value: " + record.value() +
                                ", Partition: " + record.partition() + ", Offset: " + record.offset());
                        this.index(record);
                    }
                }
            } catch (WakeupException ex) {
                LOG.info("Received WakeupException");
            } finally {
                consumer.close();
                try {
                    esClient.close();
                } catch (IOException e) {
                    LOG.error("Failed closing ElasticSearch client", e);
                    e.printStackTrace();
                }
                // tell main code we're done
                LOG.info("Consumer closed cleanly");
            }


        }

        private void shutdown() {
            LOG.info("Invoking consumer.wakeup()");
            // Interrupt consumer.poll() by throwing a WakeupException
            consumer.wakeup();
        }
    }

}
