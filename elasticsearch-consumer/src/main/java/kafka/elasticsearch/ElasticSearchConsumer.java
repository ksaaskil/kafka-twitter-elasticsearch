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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    private static final String KAFKA_TOPIC = "twitter_tweets";
    private static final String GROUP_ID = "es-consumer-1";

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final Indexer indexer;
    private final ConsumerRunnable runnable;

    private ElasticSearchConsumer(KafkaConsumer<String, String> kafkaConsumer,
                                  Indexer IIndexer) {
        this.kafkaConsumer = kafkaConsumer;
        this.indexer = IIndexer;
        this.runnable = new ConsumerRunnable(this.kafkaConsumer, this.indexer);
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

    static void run() {
        RestHighLevelClient esClient = createEsClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        Indexer indexer = new SyncIndexer(esClient);

        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer(consumer, indexer);

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

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties properties = createProperties(false);
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

    private static Properties createProperties(boolean enableAutoCommit) {
        Properties properties = new Properties();

        // Base properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest,

        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(20));

        // Auto-commit consumed messages config
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));

        return properties;
    }
    
    public interface Indexer {
        void commit(IndexRequest indexRequest);
        void commitBulk(BulkRequest bulkRequest);
    }
    
    public static class SyncIndexer implements Indexer {

        private final RestHighLevelClient esClient;

        public SyncIndexer(RestHighLevelClient esClient) {
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

        @Override
        public void commitBulk(BulkRequest bulkRequest) {
            try {
                this.esClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                LOG.error("Failed indexing bulk to ElasticSearch", e);
            }
        }

    }

    public static class ASyncIIndexer implements Indexer {

        private final RestHighLevelClient esClient;

        public ASyncIIndexer(RestHighLevelClient esClient) {
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

        @Override
        public void commitBulk(BulkRequest bulkRequest) {
            ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    LOG.info("Documents indexed with in {} milliseconds", bulkResponse.getIngestTookInMillis());
                }

                @Override
                public void onFailure(Exception e) {
                    LOG.error("Failed indexing message to ElasticSearch", e);
                }
            };

            this.esClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
        }
    }

    public static class ConsumerRunnable implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final Logger LOG = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final Indexer index;

        private ConsumerRunnable(KafkaConsumer<String, String> consumer,
                                 Indexer IIndexer) {
            this.consumer = consumer;
            this.index = IIndexer;
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
                    LOG.info("Received " + records.count() + " records.");
                    BulkRequest bulkRequest = new BulkRequest();
                    for (ConsumerRecord<String, String> record : records) {
                        LOG.debug("Key: " + record.key() + ", Value: " + record.value() +
                                ", Partition: " + record.partition() + ", Offset: " + record.offset());
                        IndexRequest indexRequest = toIndexRequest(record);
                        bulkRequest.add(indexRequest);
                        // this.index.commit(indexRequest);
                    }
                    this.index.commitBulk(bulkRequest);
                    this.consumer.commitSync();
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
