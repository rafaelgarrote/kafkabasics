package com.paradigmadigital.kafkaweminar.simpleproducerconsumer;

import com.paradigmadigital.kafkaweminar.kafkaclient.MessageProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class SimpleMessageProcessor<String> implements MessageProcessor<String,String> {

    @Override
    public void process(ConsumerRecords<String, String> records) {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("Topic: " + record.topic());
            System.out.println("Partition: " + record.partition());
            System.out.println("Offset: " + record.offset());
            System.out.println("Key: " + record.key());
            System.out.println("Value: " + record.value());
        }
    }
}
