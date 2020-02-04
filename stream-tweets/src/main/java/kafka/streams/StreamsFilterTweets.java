package kafka.streams;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {
    public static final Logger LOG = LoggerFactory.getLogger(StreamsFilterTweets.class);
    public static final int MIN_FOLLOWERS_FILTER = 100;
    public static final String FILTERED_TWEETS_TOPIC = "twitter_tweets_important";

    private static JsonObject parseAsJsonObject(String obj) {
        return JsonParser.parseString(obj).getAsJsonObject();
    }

    private static int countFollowers(String jsonTweet) {
        JsonObject parsedTweetRecord = parseAsJsonObject(jsonTweet);
        return parsedTweetRecord
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();
    }

    public static void run() {
        Properties properties = new Properties();

        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-1");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopicStream = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopicStream.filter((k, jsonTweet) -> {
            try {
                int followers = countFollowers(jsonTweet);
                return followers >= MIN_FOLLOWERS_FILTER;
            } catch (NullPointerException ex) {
                LOG.warn("Caught error", ex);
                return false;
            }
        });
        filteredStream.to(FILTERED_TWEETS_TOPIC);

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }
}
