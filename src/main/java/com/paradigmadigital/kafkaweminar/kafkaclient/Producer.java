package com.paradigmadigital.kafkaweminar.kafkaclient;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class Producer<K,V> {

    private final KafkaProducer<K,V> producer;
    private final String topic;

    public Producer(Properties props, String topic) {
        this.producer = new KafkaProducer<K, V>(props);
        this.topic = topic;
    }

    public Future<RecordMetadata> produceMessage(Message<K,V> msg, Optional<Callback> callback) {
        ProducerRecord<K,V> record = msg.getKey()
                .map(key -> new ProducerRecord<K, V>(topic, key, msg.getValue()))
                .orElse(new ProducerRecord<K,V>(topic, msg.getValue()));
        return callback.map(callbackFunction -> producer.send(record, callbackFunction))
                .orElse(producer.send(record));
    }

    public Future<Void> close() {
        return CompletableFuture.runAsync(this.producer::close);
    }
}
