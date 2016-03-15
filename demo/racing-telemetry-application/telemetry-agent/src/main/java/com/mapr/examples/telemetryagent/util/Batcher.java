package com.mapr.examples.telemetryagent.util;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class Batcher<T> {
    private List<T> batch = new LinkedList<>();
    private String name;
    private Consumer<List<T>> callback;
    private int maxSize;

    public Batcher(int maxSize, Consumer<List<T>> callback) {
        this.callback = callback;
        this.maxSize = maxSize;
    }

    public Batcher(String name, int maxSize, Consumer<List<T>> callback) {
        this.name = name;
        this.callback = callback;
        this.maxSize = maxSize;
    }

    public void add(T value) {
        batch.add(value);
        if (batch.size() >= maxSize) {
            callback.accept(batch);
            batch.clear();
        }
    }

    @Override
    public String toString() {
        return "Batcher{" +
                "name='" + name + '\'' +
                "size='" + batch.size() + '\'' +
                '}';
    }
}
