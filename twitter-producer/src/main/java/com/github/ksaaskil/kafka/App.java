/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.github.ksaaskil.kafka;

import com.github.ksaaskil.kafka.twitter.TwitterProducer;

public class App {
    public String getGreeting() {
        String value = System.getenv("MY_KEY");
        if (value == null) {
            throw new RuntimeException("Missing environment variable MY_KEY");
        }
        return value;
    }

    public static void main(String[] args) {
        TwitterProducer.run();
    }
}
