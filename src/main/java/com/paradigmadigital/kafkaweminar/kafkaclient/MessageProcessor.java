package com.paradigmadigital.kafkaweminar.kafkaclient;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MessageProcessor<K,V> {

    public void process(ConsumerRecords<K, V> records);

}
