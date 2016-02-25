package com.mapr.examples.telemetryagent.util;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

public class Batcher<T> {
    public static final int SIZE = 15;
    private List<T> batch = new LinkedList<>();
    private String name;
    private Consumer<List<T>> callback;

    public Batcher(Consumer<List<T>> callback) {
        this.callback = callback;
    }

    public Batcher(String name, Consumer<List<T>> callback) {
        this.name = name;
        this.callback = callback;
    }

    public void add(T value) {
        batch.add(value);
//        System.out.println(this + ": item added");
        if (batch.size() >= SIZE) {
//            System.out.println(this + ": flushing batch");
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
