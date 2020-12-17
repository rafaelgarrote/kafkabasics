package com.paradigmadigital.kafkaweminar.kafkaclient;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.util.function.Function;

public class Message<K,V> {

    private final Optional<K> key;
    private final V value;

    public Message(Optional<K> key, V value) {
        this.key = key;
        this.value = value;
    }

    @NotNull
    @Contract(pure = true)
    public static <K,V> Function<Optional<K>, Function<V, Message<K,V>>> apply() {
        return key -> value -> new Message<K, V>(key, value);
    }

    public Optional<K> getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}
