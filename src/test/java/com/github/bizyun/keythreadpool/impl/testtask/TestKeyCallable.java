package com.github.bizyun.keythreadpool.impl.testtask;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.bizyun.keythreadpool.KeyCallable;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
public class TestKeyCallable<Void> implements KeyCallable<Void> {

    private final long key;
    private final AtomicInteger count;
    private final ConcurrentHashMap<Long, LinkedBlockingQueue<Integer>> consumerRecords;
    private final int value;

    public TestKeyCallable(long key, AtomicInteger count,
            int value, ConcurrentHashMap<Long, LinkedBlockingQueue<Integer>> consumerRecords) {
        this.key = key;
        this.count = count;
        this.value = value;
        this.consumerRecords = consumerRecords;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public Void call() throws Exception {
        count.incrementAndGet();
        LinkedBlockingQueue<Integer> records = consumerRecords.get(key);
        if (records == null) {
            records = consumerRecords.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());
        }
        records.offer(value);
        return null;
    }
}
