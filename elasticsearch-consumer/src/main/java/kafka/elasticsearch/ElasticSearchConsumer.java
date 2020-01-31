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

    private static Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static String KAFKA_TOPIC = "twitter_tweets";
    private static String GROUP_ID = "es-consumer-1";

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final IndexingStrategy indexingStrategy;
    private final ConsumerRunnable runnable;

    private ElasticSearchConsumer(KafkaConsumer<String, String> kafkaConsumer,
                                  IndexingStrategy indexingStrategy) {
        this.kafkaConsumer = kafkaConsumer;
        this.indexingStrategy = indexingStrategy;
        this.runnable = new ConsumerRunnable(this.kafkaConsumer, this.indexingStrategy);
    }

    private Future start() {
        LOG.info("Creating consumer thread");
        ExecutorService executor = Executors.newSingleThreadExecutor();

        Future future = executor.submit(this.runnable);

        Thread shutdownHook = new Thread(() -> {
            runnable.shutdown();

            try {
                executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
                LOG.info("Exiting cleanly");
            } catch (InterruptedException e) {
                LOG.error("Did not exit cleanly", e);
            }
        });

        LOG.info("Adding shutdown hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        return future;

    }

    static void run(boolean syncProcessing) {
        RestHighLevelClient esClient = createEsClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer(syncProcessing);

        IndexingStrategy indexingStrategy = syncProcessing ? new SyncIndexingStrategy(esClient) : new ASyncIndexingStrategy(esClient);

        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer(consumer, indexingStrategy);

        Future future = elasticSearchConsumer.start();

        try {
            future.get();
        } catch (InterruptedException e) {
            LOG.info("Interrupted");
        } catch (ExecutionException e) {
            LOG.info("Failed executing", e);
        } finally {
            consumer.close();
            try {
                esClient.close();
            } catch (IOException e) {
                LOG.error("Failed closing ElasticSearch client", e);
            }
            // tell main code we're done
            LOG.info("Consumer closed cleanly");
        }
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(boolean sync) {
        Properties properties = createProperties();
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(sync));
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
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

        // For synchronous processing
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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
    
    public interface IndexingStrategy {
        void commit(IndexRequest indexRequest);
    }
    
    public static class SyncIndexingStrategy implements IndexingStrategy {

        private final RestHighLevelClient esClient;

        public SyncIndexingStrategy(RestHighLevelClient esClient) {
            this.esClient = esClient;
        }

        @Override
        public void commit(IndexRequest indexRequest) {
            try {
                this.esClient.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                LOG.error("Failed indexing message to ElasticSearch", e);
            }
        }
    }

    public static class ASyncIndexingStrategy implements IndexingStrategy {

        private final RestHighLevelClient esClient;

        public ASyncIndexingStrategy(RestHighLevelClient esClient) {
            this.esClient = esClient;
        }

        public void commit(IndexRequest indexRequest) {
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
    }

    public static class ConsumerRunnable implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final IndexingStrategy index;

        private ConsumerRunnable(KafkaConsumer<String, String> consumer,
                                 IndexingStrategy indexingStrategy) {
            this.consumer = consumer;
            this.index = indexingStrategy;
        }

        private static JsonObject parseAsJsonObject(String obj) {
            return JsonParser.parseString(obj).getAsJsonObject();
        }

        private static IndexRequest toIndexRequest(ConsumerRecord<String, String> tweet) {
            // Kafka generic ID
            // String id = record.topic() + record.partition() + record.offset();

            String tweetRecordString = tweet.value();

            JsonObject parsedTweetRecord = parseAsJsonObject(tweetRecordString);

            String text = parsedTweetRecord.get("text").getAsString();

            // Twitter ID
            String twitterId = parsedTweetRecord.get("id_str").getAsString();

            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("id", twitterId);
            jsonMap.put("text", text);

            return new IndexRequest("twitter")
                    .id(twitterId)
                    .source(jsonMap);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        LOG.debug("Key: " + record.key() + ", Value: " + record.value() +
                                ", Partition: " + record.partition() + ", Offset: " + record.offset());
                        IndexRequest indexRequest = toIndexRequest(record);
                        this.index.commit(indexRequest);
                    }
                }
            } catch (WakeupException ex) {
                LOG.info("Received WakeupException");
            }
        }

        private void shutdown() {
            LOG.info("Invoking consumer.wakeup()");
            // Interrupt consumer.poll() by throwing a WakeupException
            consumer.wakeup();
        }
    }

}
