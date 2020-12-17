package com.paradigmadigital.kafkaweminar.twitterclient;

import com.paradigmadigital.kafkaweminar.kafkaclient.Message;
import com.paradigmadigital.kafkaweminar.kafkaclient.Producer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import twitter4j.*;

import java.util.Optional;

public class RandomListener implements StatusListener {

    private final Producer<String, String> producer;

    public RandomListener(Producer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public void onStatus(Status status) {
        System.out.println("----------------------------");
        System.out.println(status);
        System.out.println(TwitterObjectFactory.getRawJSON(status));
        System.out.println("----------------------------");
        producer.produceMessage(
                new Message<String, String>(Optional.empty(), TwitterObjectFactory.getRawJSON(status)),
                Optional.of(new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        e.printStackTrace();
                    }
                }));
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }
}
