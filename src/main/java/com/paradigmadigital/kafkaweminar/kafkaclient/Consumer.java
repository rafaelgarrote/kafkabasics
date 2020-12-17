package com.paradigmadigital.kafkaweminar.kafkaclient;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class Consumer<K,V> {

    private final KafkaConsumer<K,V> consumer;
    private final List<String> topics;

    public Consumer(Properties props, List<String> topics) {
        this.consumer = new KafkaConsumer<K,V>(props);
        this.topics = topics;
    }

    public void subscribeAndConsume(Duration timeout, MessageProcessor<K,V> msgProcessor) {
        this.consumer.subscribe(this.topics);
        try {
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(timeout);
                msgProcessor.process(records);
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        System.out.println("-----Committing last offsets: " + currentOffsets);
        try {
            consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                public void onComplete(Map<TopicPartition,
                        OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("Commit failed for offsets: " + offsets);
                        exception.printStackTrace();
                        consumer.commitSync(currentOffsets);
                    } else {
                        System.out.println("Offsets commit: " + offsets);
                    }
                }
            });
        } catch (Exception e) {
            consumer.commitSync(currentOffsets);
        }
    }

    public Future<Void> close() {
        return CompletableFuture.runAsync(this.consumer::wakeup);
    }

}
