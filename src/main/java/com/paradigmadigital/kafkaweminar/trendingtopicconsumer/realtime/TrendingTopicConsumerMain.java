package com.paradigmadigital.kafkaweminar.trendingtopicconsumer.realtime;

import com.paradigmadigital.kafkaweminar.AppConfiguration;
import com.paradigmadigital.kafkaweminar.kafkaclient.Consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TrendingTopicConsumerMain extends AppConfiguration {

    static public void main(String[] args) {
        Properties kafkaProperties = getKafkaConsumerProperties();
        List<String> topics = new ArrayList<>();
        topics.add(getTwitterTopic());
        Duration d = Duration.ofMillis(getPollInterval());
        Consumer<String, String> consumer = new Consumer<>(kafkaProperties, topics);
        consumer.subscribeAndConsume(d, new TrendingTopicMessageProcessor());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                consumer.close();
            }
        });
    }
}
