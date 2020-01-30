package kafka.elasticsearch;

import java.io.IOException;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

    public static Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
    public static String KAFKA_TOPIC = "twitter_tweets";
    private final RestHighLevelClient esClient;

    public ElasticSearchConsumer(RestHighLevelClient esClient) {
        this.esClient = esClient;
    }

    protected Cancellable sendTestJson() {
        String jsonString = "{ \"foo\": \"bar\"}";
        IndexRequest indexRequest = new IndexRequest("twitter")
            .source(jsonString, XContentType.JSON);

        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                LOG.debug("Document indexed");
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Failed indexing message to ElasticSearch", e);
            }
        };

        Cancellable cancellable = this.esClient.indexAsync(indexRequest, RequestOptions.DEFAULT, listener);
        return cancellable;
    }

    protected static void run() {

        RestHighLevelClient esClient = createClient();

        ElasticSearchConsumer elasticSearchConsumer = new ElasticSearchConsumer(esClient);

        elasticSearchConsumer.sendTestJson();

        Thread shutdownHook = new Thread(() -> {
            LOG.info("Closing clients...");

            try {
                LOG.info("Closing ES client...");
                esClient.close();
                LOG.info("Closed ES client");
            } catch (IOException e) {
                LOG.error("Failed closing ES client");
                e.printStackTrace();
            }

            /*
             * try { executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
             * logger.info("Exiting cleanly"); } catch (InterruptedException e) {
             * logger.error("Did not exit cleanly", e); }
             */
        });

        LOG.info("Adding shutdown hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // process(client, msgQueue, kafkaProducer);
    }

    private static RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        );
        return new RestHighLevelClient(builder);
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

//        // Base properties
//        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
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

}
