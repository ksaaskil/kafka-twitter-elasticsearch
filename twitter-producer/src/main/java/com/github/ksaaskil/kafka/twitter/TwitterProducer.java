package com.github.ksaaskil.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {

    public static Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);
    public static String KAFKA_TOPIC = "twitter_tweets";

    public static void run() {
        // Create a Twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = client(msgQueue);

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Thread shutdownHook = new Thread(() -> {
            LOG.info("Shutting down Twitter client...");
            client.stop();
            LOG.info("Closing Kafka producer...");
            kafkaProducer.close();
            LOG.info("Done.");

            /*
             * try { executor.awaitTermination(10000, TimeUnit.MILLISECONDS);
             * logger.info("Exiting cleanly"); } catch (InterruptedException e) {
             * logger.error("Did not exit cleanly", e); }
             */
        });

        Runtime.getRuntime().addShutdownHook(shutdownHook);

        process(client, msgQueue, kafkaProducer);
    }

    private static Properties createProperties() {
        Properties properties = new Properties();

        // Base properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe producer properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High-throughput producer properties
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // lz4, gzip, etc.
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "65536");  // 64 kB
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");  // Linger a while for larger batches

        return properties;
    }

    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = createProperties();
        return new KafkaProducer<>(properties);
    }

    /**
     * Process messages. Connects the Twitter client. Does NOT stop or close
     * clients.
     * 
     * @param hosebirdClient Twitter client.
     * @param msgQueue       Message queue of the client.
     * @param kafkaProducer  Kafka producer.
     */
    public static void process(Client hosebirdClient, BlockingQueue<String> msgQueue,
            KafkaProducer<String, String> kafkaProducer) {
        LOG.info("Connecting to Twitter...");
        hosebirdClient.connect();
        LOG.info("Connected");
        try {
            while (!hosebirdClient.isDone()) {
                LOG.debug("Polling messages...");
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);

                if (msg == null) {
                    LOG.debug("No messages");
                    continue;
                }

                LOG.debug("Got message: {}", msg);
                ProducerRecord<String, String> record = createKafkaProducerRecord(msg);
                kafkaProducer.send(record, (RecordMetadata metadata, Exception exception) -> {
                    if (exception != null) {
                        LOG.error("Failed sending message to Kafka", exception);
                    } else {
                        LOG.debug("Message sent to Kafka");
                    }
                });
            }
        } catch (InterruptedException ex) {
            LOG.error("Processing interrupted", ex);
        }

    }

    public static ProducerRecord<String, String> createKafkaProducerRecord(String msg) {
        return new ProducerRecord<>(KAFKA_TOPIC, null, msg);
    }

    public static Client client(BlockingQueue<String> msgQueue) {
        Authentication auth = createAuth();

        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(100000);

        /**
         * Declare the host you want to connect to, the endpoint, and authentication
         * (basic auth or oauth)
         */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Terms to follow
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // Optional: set up some followings
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // hosebirdEndpoint.followings(followings);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01").hosts(hosebirdHosts).authentication(auth)
                .endpoint(hosebirdEndpoint).processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);

        return builder.build();
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

    public static Authentication createAuth() {
        String consumerKey = readEnv("TWITTER_CONSUMER_API_KEY");
        String consumerSecret = readEnv("TWITTER_CONSUMER_SECRET");
        String token = readEnv("TWITTER_TOKEN");
        String tokenSecret = readEnv("TWITTER_TOKEN_SECRET");

        return new OAuth1(consumerKey, consumerSecret, token, tokenSecret);
    }

}
