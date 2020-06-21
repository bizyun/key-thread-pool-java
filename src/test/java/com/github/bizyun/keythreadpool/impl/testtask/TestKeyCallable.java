package com.github.bizyun.keythreadpool.impl.testtask;

import java.util.concurrent.atomic.AtomicInteger;

import com.github.bizyun.keythreadpool.KeyCallable;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
public class TestKeyCallable<Void> implements KeyCallable<Void> {

    private final int key;
    private final AtomicInteger count;

    public TestKeyCallable(int key, AtomicInteger count) {
        this.key = key;
        this.count = count;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public Void call() throws Exception {
        count.incrementAndGet();
        return null;
    }
}
