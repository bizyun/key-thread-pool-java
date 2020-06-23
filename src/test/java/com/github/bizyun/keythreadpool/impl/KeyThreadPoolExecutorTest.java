package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.keythreadpool.impl.KeyThreadPoolExecutor.newKeyThreadPool;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.keythreadpool.impl.testtask.TestKeyCallable;
import com.github.bizyun.keythreadpool.impl.testtask.TestKeyRunnable;
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
        ConcurrentHashMap<Long, LinkedBlockingQueue<Integer>> consumerRecords = new ConcurrentHashMap<>();
        int taskCount = 30000;
        AtomicInteger runTaskCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.execute(new TestKeyRunnable(key, runTaskCount, key, consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.submit(new TestKeyRunnable(key, runTaskCount, key, consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                keyExecutor.submit(new TestKeyCallable<>(key, runTaskCount, key, consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                CompletableFuture.runAsync(new TestKeyRunnable(key, runTaskCount, key, consumerRecords), keyExecutor);
            });
        }
        ListeningExecutorService keyListeningExecutor = MoreExecutors.listeningDecorator(keyExecutor);
        AtomicInteger callbackCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i;
            producerExecutor.execute(() -> {
                ListenableFuture<Object> future = keyListeningExecutor.submit(new TestKeyCallable<>(key, runTaskCount, key, consumerRecords));
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
                TestKeyRunnable runner = new TestKeyRunnable(key, runTaskCount, key, consumerRecords);
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
                TestKeyCallable<Void> callable = new TestKeyCallable<>(key, runTaskCount, key, consumerRecords);
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
                TestKeyRunnable runner = new TestKeyRunnable(key, runTaskCount, key, consumerRecords);
                keyExecutor.submit(() -> runner.run());
            });
        }

        shutdownAndAwaitTermination(producerExecutor, 1, TimeUnit.HOURS);
        assertEquals(3, ((KeyThreadPoolExecutor) keyExecutor).getCorePoolSize());
        shutdownAndAwaitTermination(keyExecutor, 1, TimeUnit.HOURS);
        assertEquals(taskCount * 8, runTaskCount.get());
        assertEquals(taskCount, callbackCount.get());
        assertEquals(taskCount, consumerRecords.size());
        for (int i = 0; i < taskCount; i++) {
            long key = i;
            LinkedBlockingQueue<Integer> records = consumerRecords.get(key);
            assertEquals(8, records.size());
            for (int j = 0; j < 8; j++) {
                assertEquals(i, records.poll());
            }
        }
    }

    @Test
    void test2() {
        AtomicInteger poolSize = new AtomicInteger(1);
        AtomicInteger queueCapacity = new AtomicInteger(100);
        AtomicInteger queueCount = new AtomicInteger(10);

        ExecutorService keyExecutor = newKeyThreadPool(() -> poolSize.get(),
                () -> new LinkedBlockingQueue<>(queueCapacity.get()), () -> queueCount.get());
        ConcurrentHashMap<Long, LinkedBlockingQueue<Integer>> consumerRecords = new ConcurrentHashMap<>();
        int taskCount = 30000;
        int keyMod = 17;
        Map<Integer, ExecutorService> producerExecutorMap = new HashMap<>();
        for (int i = 0; i < 17; i++) {
            producerExecutorMap.put(i, Executors.newFixedThreadPool(1));
        }

        AtomicInteger runTaskCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                keyExecutor.execute(new TestKeyRunnable(key, runTaskCount, value, consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                keyExecutor.submit(new TestKeyRunnable(key, runTaskCount, value, consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                keyExecutor.submit(new TestKeyCallable<>(key, runTaskCount, value,
                        consumerRecords));
            });
        }
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                CompletableFuture.runAsync(new TestKeyRunnable(key, runTaskCount, value,
                        consumerRecords), keyExecutor);
            });
        }
        ListeningExecutorService keyListeningExecutor = MoreExecutors.listeningDecorator(keyExecutor);
        AtomicInteger callbackCount = new AtomicInteger();
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                ListenableFuture<Object> future =
                        keyListeningExecutor.submit(new TestKeyCallable<>(key, runTaskCount, value,
                                consumerRecords));
                future.addListener(() -> {
                    callbackCount.incrementAndGet();
                }, MoreExecutors.directExecutor());
            });
        }

        // 扩容队列数
        queueCount.set(1000);
        queueCapacity.set(300);
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                TestKeyRunnable runner = new TestKeyRunnable(key, runTaskCount, value, consumerRecords);
                keyExecutor.execute(() -> runner.run());
            });
        }

        // 扩容线程数，缩容队列数
        poolSize.set(20);
        queueCount.set(2);
        queueCapacity.set(200000);
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                TestKeyCallable<Void> callable = new TestKeyCallable<>(key, runTaskCount, value,
                        consumerRecords);
                keyExecutor.submit(() -> callable.call());
            });
        }

        // 缩容线程数，扩容队列数
        poolSize.set(3);
        queueCount.set(20);
        queueCapacity.set(200100);
        for (int i = 0; i < taskCount; i++) {
            final int key = i % keyMod;
            final int value = i;
            producerExecutorMap.get(key).execute(() -> {
                TestKeyRunnable runner = new TestKeyRunnable(key, runTaskCount, value, consumerRecords);
                keyExecutor.submit(() -> runner.run());
            });
        }
        producerExecutorMap.values().forEach(producerExecutor -> shutdownAndAwaitTermination(producerExecutor, 1, TimeUnit.HOURS));
        assertEquals(3, ((KeyThreadPoolExecutor) keyExecutor).getCorePoolSize());
        shutdownAndAwaitTermination(keyExecutor, 1, TimeUnit.HOURS);
        assertEquals(taskCount * 8, runTaskCount.get());
        assertEquals(taskCount, callbackCount.get());
        assertEquals(keyMod, consumerRecords.size());
        for (int j = 0; j < 8; j++) {
            for (int i = 0; i < taskCount; i++) {
                long key = i % keyMod;
                LinkedBlockingQueue<Integer> records = consumerRecords.get(key);
                assertEquals(i, records.poll());
            }
        }
    }
}