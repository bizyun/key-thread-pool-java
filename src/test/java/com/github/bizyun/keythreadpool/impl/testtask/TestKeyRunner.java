package com.github.bizyun.keythreadpool.impl.testtask;

import java.util.concurrent.atomic.AtomicInteger;

import com.github.bizyun.keythreadpool.KeyRunner;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
public class TestKeyRunner implements KeyRunner {

    private final int key;
    private final AtomicInteger count;

    public TestKeyRunner(int key, AtomicInteger count) {
        this.key = key;
        this.count = count;
    }

    @Override
    public long getKey() {
        return key;
    }

    @Override
    public void run() {
        count.incrementAndGet();
    }
}
