package com.paradigmadigital.kafkaweminar;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Properties;

public abstract class AppConfiguration {

    private static final Config conf = ConfigFactory.load();

    public static String getTwitterConsumerKey() {
        return conf.getString("twitter.api.consumer.key");
    }

    public static String getTwitterConsumerSecret() {
        return conf.getString("twitter.api.consumer.secret");
    }

    public static String getTwitterAccessToken() {
        return conf.getString("twitter.api.consumer.access.token");
    }

    public static String getTwitterAccessTokenSecret() {
        return conf.getString("twitter.api.consumer.access.secret");
    }

    public static Properties getKafkaProducerProperties() {
        return confToProperties(conf.getConfig("producer.kafka"));
    }

    public static Properties getKafkaConsumerProperties() {
        return confToProperties(conf.getConfig("consumer.kafka"));
    }

    public static Properties getTrendingTopicProcessorProperties() { return confToProperties(conf.getConfig("tt")); }

    public static String getTwitterTopic() { return conf.getString("twitter.topic"); }

    public static Long getPollInterval() { return conf.getLong("consumer.poll.interval.millis"); }

    private static Properties confToProperties(Config conf) {
        Properties properties = new Properties();
        conf.entrySet()
                .stream()
                .iterator()
                .forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().unwrapped()));
        return properties;
    }


}
