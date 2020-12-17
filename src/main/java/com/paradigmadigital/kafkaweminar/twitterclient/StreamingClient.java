package com.paradigmadigital.kafkaweminar.twitterclient;

import twitter4j.StreamListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class StreamingClient {

    private Optional<TwitterStream> twitterStream = Optional.empty();
    private static Optional<StreamingClient> client = Optional.empty();

    private StreamingClient(Configuration conf, StreamListener ... listeners) {
        this.twitterStream = this.twitterStream.or(
            () -> Optional.of(new TwitterStreamFactory(conf).getInstance())
        );
        this.twitterStream.ifPresent(ts -> Arrays.stream(listeners).iterator().forEachRemaining(ts::addListener));
        twitterStream.map(TwitterStream::sample);
        client = Optional.of(this);
    }

    public static StreamingClient getInstance(Configuration conf, StreamListener ... listeners) {
        return client.orElse(new StreamingClient(conf, listeners));
    }

    public static Optional<StreamingClient> getOptInstance(Configuration conf, StreamListener ... listeners) {
        return client.or(() -> Optional.of(new StreamingClient(conf, listeners)));
    }

    public Future<Void> close() {
        return CompletableFuture.runAsync(() -> this.twitterStream.map(TwitterStream::shutdown));
    }
}
