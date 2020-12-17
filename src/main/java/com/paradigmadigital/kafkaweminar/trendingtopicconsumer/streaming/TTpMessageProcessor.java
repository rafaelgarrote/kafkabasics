package com.paradigmadigital.kafkaweminar.trendingtopicconsumer.streaming;

import com.paradigmadigital.kafkaweminar.kafkaclient.Consumer;
import com.paradigmadigital.kafkaweminar.kafkaclient.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.javatuples.Pair;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TTpMessageProcessor implements MessageProcessor<String, String> {

    private static final String TT_WINDOW_SECONDS = "window.seconds";
    private static final String TT_LIMIT = "limit";
    private static final String TT_THROUGHPUT_MILLIS = "throughput.millis";

    private final Queue<Pair<String, ConsumerRecord<String, String>>> queue = new LinkedList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public TTpMessageProcessor(Properties properties, Consumer<String, String> consumer) {
        Integer windowTime = (Integer) properties.get(TT_WINDOW_SECONDS);
        Integer ttLimit = (Integer) properties.get(TT_LIMIT);
        Integer throughput = (Integer) properties.get((TT_THROUGHPUT_MILLIS));
        check(windowTime, ttLimit, throughput, consumer);
    }

    private Future<Void> check(long windowTime, int ttLimit, long throughput, Consumer<String, String> consumer) {
        return executor.submit(() -> {
            Map<Integer, Long> offsets = new HashMap<>();
            Map<String, List<ConsumerRecord<String, String>>> messages = new ConcurrentHashMap<>();
            Optional<Date> lastWindow = Optional.empty();
            Optional<String> topic = Optional.empty();
            System.out.println("-----------Running");

            while (true) {
                try {
//                    System.out.println("------------" + queue.size());
                    if( !queue.isEmpty() ) {
                        synchronized(queue) {
                            Pair<String, ConsumerRecord<String, String>> message = queue.remove();
                            if ( topic.isEmpty() ) { topic = Optional.ofNullable(message.getValue1().topic()); }
                            offsets = updateOffsets(message.getValue1(), offsets);
                            Status s = TwitterObjectFactory.createStatus(message.getValue1().value());
                            if ( lastWindow.isEmpty() ) {
                                Date initialDateWindow = s.getCreatedAt();
                                initialDateWindow.setSeconds(0);
                                lastWindow = Optional.of(initialDateWindow);
                                System.out.println("----New Window: " + lastWindow.get());
                            }
                            Date createdAt = s.getCreatedAt();

                            List<ConsumerRecord<String, String>> q = Optional
                                    .ofNullable(messages.get(message.getValue0()))
                                    .orElse(new ArrayList<>());
                            q.add(message.getValue1());
                            messages.put(message.getValue0(), q);

//                            System.out.println(createdAt.get());
//                            System.out.println(lastWindow.get());
                            long diff = ChronoUnit.SECONDS.between(lastWindow.get().toInstant(), createdAt.toInstant());
//                            System.out.println("----Diff: " + diff);
                            if (diff >= windowTime) {
                                computeResult(ttLimit, messages, lastWindow);
                                commitOffsets(topic.get(), offsets, consumer);
                                lastWindow = Optional.empty();
                            }
                        }
                    }
                    Thread.sleep(throughput);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void computeResult(
            int ttLimit,
            Map<String, List<ConsumerRecord<String, String>>> messagesToProcess,
            Optional<Date> window) {
        for(String lang: messagesToProcess.keySet()) {
            Map<String, Long> tt = new HashMap<>();
            List<ConsumerRecord<String, String>> queue = messagesToProcess.get(lang);
            List<String> hashtags = queue.stream().flatMap(extractHashtags).collect(Collectors.toList());
            if( !hashtags.isEmpty() ) {
                tt = computeTrendingTopic(hashtags, tt);
                List<Pair<String, Long>> ll = new ArrayList<>();
                tt.forEach((k, v) -> ll.add(new Pair<String, Long>(k, v)));
                System.out.println("-----[TT for: " + lang + "-" + window.get() + " ]-----");
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
                }).limit(ttLimit).forEach(System.out::println);
            }
        }
    }

    private static final Function<ConsumerRecord<String, String>,
            Stream<String>> extractHashtags =
            (record) -> {
                Stream<String> hashtags = Stream.empty();
                try {
                    Status status = TwitterObjectFactory.createStatus(record.value());
                    hashtags = Arrays.stream(status.getHashtagEntities())
                            .map(hashtag -> hashtag.getText().toLowerCase());
                } catch (Exception e) {

                }
                return hashtags;
            };

    private Map<String, Long> computeTrendingTopic(
            List<String> hashtags,
            Map<String, Long> tt) {
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

    private Map<Integer, Long> updateOffsets(ConsumerRecord<String, String> record, Map<Integer, Long> offsets) {
        Long latestOffset = Optional.ofNullable(offsets.get(record.partition()))
                .map(offset -> {
                    if (record.offset() > offset) {
                        return record.offset();
                    } else {
                        return offset;
                    }
                }).orElse(record.offset());
        offsets.put(record.partition(), latestOffset);
        return offsets;
    }

    private void commitOffsets(String topic, Map<Integer, Long> offsets, Consumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        for( Integer partition: offsets.keySet()) {
            currentOffsets.put(
                    new TopicPartition(topic, partition),
                    new OffsetAndMetadata(offsets.get(partition) + 1, "no metadata"));
        }

        consumer.commitOffsets(currentOffsets);
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
//        System.out.println("New records: " + records.count());
        synchronized(queue) {
            records.forEach(record -> queue.add(new Pair<>(record.key(), record)));
        }
    }

}