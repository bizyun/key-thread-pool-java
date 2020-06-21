package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.keythreadpool.impl.KeyThreadPoolExecutor.newKeyThreadPool;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.keythreadpool.impl.testtask.TestKeyCallable;
import com.github.bizyun.keythreadpool.impl.testtask.TestKeyRunner;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
class KeyThreadPoolExecutorTest {

    @Test
    void test1() {
        AtomicInteger poolSize = new AtomicInteger(1);
        AtomicInteger queueCapacity = new AtomicInteger(10);
        AtomicInteger queueCount = new AtomicInteger(3);

        ExecutorService keyExecutor = newKeyThreadPool(() -> poolSize.get(),
                () -> new LinkedBlockingQueue<>(queueCapacity.get()), () -> queueCount.get());
        ExecutorService producerExecutor = Executors.newFixedThreadPool(10);
        int taskCount = 100000;
        AtomicInteger runTaskCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.execute(new TestKeyRunner(key, runTaskCount));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.submit(new TestKeyRunner(key, runTaskCount));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.submit(new TestKeyCallable<>(key, runTaskCount));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                CompletableFuture.runAsync(new TestKeyRunner(key, runTaskCount), keyExecutor);
            });
        }
        ListeningExecutorService keyListeningExecutor = MoreExecutors.listeningDecorator(keyExecutor);
        AtomicInteger callbackCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                ListenableFuture<Object> future = keyListeningExecutor.submit(new TestKeyCallable<>(key, runTaskCount));
                future.addListener(() -> {
                    callbackCount.incrementAndGet();
                }, MoreExecutors.directExecutor());
            });
        }

        // 扩容队列数
        queueCapacity.set(30);
        queueCount.set(10);
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                TestKeyRunner runner = new TestKeyRunner(key, runTaskCount);
                keyExecutor.execute(() -> runner.run());
            });
        }

        // 扩容线程数，缩容队列数
        poolSize.set(8);
        queueCount.set(2);
        queueCapacity.set(1000);
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                TestKeyCallable<Void> callable = new TestKeyCallable<>(key, runTaskCount);
                keyExecutor.submit(() -> callable.call());
            });
        }

        // 缩容线程数，扩容队列数
        poolSize.set(3);
        queueCount.set(20);
        queueCapacity.set(1000);
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                TestKeyRunner runner = new TestKeyRunner(key, runTaskCount);
                keyExecutor.submit(() -> runner.run());
            });
        }


        shutdownAndAwaitTermination(producerExecutor, 1, TimeUnit.HOURS);
        assertEquals(3, ((KeyThreadPoolExecutor) keyExecutor).getCorePoolSize());
        shutdownAndAwaitTermination(keyExecutor, 1, TimeUnit.HOURS);
        assertEquals(taskCount * 8, runTaskCount.get());
        assertEquals(taskCount, callbackCount.get());
    }
}