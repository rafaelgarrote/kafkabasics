package com.paradigmadigital.kafkaweminar.trendingtopicconsumer.realtime;

import com.paradigmadigital.kafkaweminar.kafkaclient.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.javatuples.Pair;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.*;

public class TrendingTopicMessageProcessor implements MessageProcessor<java.lang.String, java.lang.String> {

    private Map<String, List<ConsumerRecord<String, String>>> messages = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public TrendingTopicMessageProcessor() {
        final ScheduledFuture<?> trendingTopicHandle =
                scheduler.scheduleAtFixedRate(this::computeResult, 1, 300, SECONDS);
    }

    private void computeResult() {
        Map<String, List<ConsumerRecord<String, String>>> messagesToProcess = messages;
        messages = new ConcurrentHashMap<>();
        Map<Integer, Long> offsets = new HashMap<>();

        for(String lang: messagesToProcess.keySet()) {
            Map<java.lang.String, Long> tt = new HashMap<>();
            List<ConsumerRecord<String, String>> queue = messagesToProcess.get(lang);
            List<String> hashtags = queue.stream().flatMap(extractHashtags).collect(Collectors.toList());
            offsets = updateOffsets(queue, offsets);
            if( !hashtags.isEmpty() ) {
                tt = computeTrendingTopic(hashtags, tt);
                List<Pair<String, Long>> ll = new ArrayList<>();
                tt.forEach((k, v) -> ll.add(new Pair<String, Long>(k, v)));
                System.out.println("-----[TT for: " + lang +  " ]-----");
                ll.stream().sorted(new Comparator<Pair<String, Long>>() {
                    @Override
                    public int compare(final Pair<String, Long> o1, final Pair<String, Long> o2) {
                        if (o1.getValue1() > o2.getValue1()) {
                            return -1;
                        } else if (o1.getValue1().equals(o2.getValue1())) {
                            return 0;
                        } else {
                            return 1;
                        }
                    }
                }).limit(10).forEach(System.out::println);
            }
        }
        System.out.println("--------Offsets-----");
        System.out.println(offsets);
    }

    private static final Function<ConsumerRecord<java.lang.String, java.lang.String>,
            Stream<java.lang.String>> extractHashtags =
        (record) -> {
            Stream<java.lang.String> hashtags = Stream.empty();
            try {
                Status status = TwitterObjectFactory.createStatus(record.value());
                hashtags = Arrays.stream(status.getHashtagEntities())
                        .map(hashtag -> hashtag.getText().toLowerCase());
            } catch (Exception e) {

            }
            return hashtags;
        };

    private Map<java.lang.String, Long> computeTrendingTopic(
            List<String> hashtags,
            Map<java.lang.String, Long> tt) {
        if ( hashtags.isEmpty() ) {
            return tt;
        } else {
            String head = hashtags.get(0);
            Long sum = Optional.ofNullable(tt.get(head)).map(hashtag -> hashtag + 1).orElse(1L);
            tt.put(head, sum);
            List<String> tail = new ArrayList<>(hashtags);
            tail.remove(0);
            return computeTrendingTopic(tail, tt);
        }
    }

    private Map<Integer, Long> updateOffsets(List<ConsumerRecord<String, String>> records, Map<Integer, Long> offsets) {
        if( records.isEmpty() ) {
            return offsets;
        } else {
            ConsumerRecord<String, String> head = records.get(0);
            Long latestOffset = Optional.ofNullable(offsets.get(head.partition()))
                    .map(offset -> {
                        if (head.offset() > offset) {
                            return head.offset();
                        } else {
                            return offset;
                        }
                    }).orElse(head.offset());
            offsets.put(head.partition(), latestOffset);
            List<ConsumerRecord<String, String>> tail = new ArrayList<>(records);
            tail.remove(0);
            return updateOffsets(tail, offsets);
        }
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                Optional.ofNullable(messages.get(record.key()))
                        .map(q -> q.add(record))
                        .orElse(createQueue(messages, record.key()).add(record));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private List<ConsumerRecord<String, String>> createQueue(
            Map<String, List<ConsumerRecord<String, String>>> messages,
            String key) {
        List<ConsumerRecord<String, String>> queue = new ArrayList<>();
        messages.put(key, queue);
        return queue;
    }

}
