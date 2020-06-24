package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.keythreadpool.impl.KeyThreadPoolExecutor.newKeyThreadPool;
import static com.google.common.util.concurrent.MoreExecutors.shutdownAndAwaitTermination;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.keythreadpool.KeyRunnable;
import com.github.bizyun.keythreadpool.KeySupplier;
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
        assertEquals(taskCount * 8, ((KeyThreadPoolExecutor) keyExecutor).getCompletedTaskCount());
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
        queueCapacity.set(400000);
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
        queueCapacity.set(400100);
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
        assertEquals(taskCount * 8, ((KeyThreadPoolExecutor) keyExecutor).getCompletedTaskCount());
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

    @Test
    void test3() throws InterruptedException {
        AtomicInteger poolSize = new AtomicInteger(10);
        AtomicInteger queueCapacity = new AtomicInteger(1);
        AtomicInteger queueCount = new AtomicInteger(100);

        ExecutorService keyExecutor = newKeyThreadPool(() -> poolSize.get(),
                () -> new LinkedBlockingQueue<>(queueCapacity.get()), () -> queueCount.get());
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 100; i++) {
            final int k = i;
            keyExecutor.execute(new KeyRunnable() {
                @Override
                public long getKey() {
                    return k;
                }

                @Override
                public void run() {
                    latch.countDown();
                    try {
                        Thread.sleep(1000000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        latch.await();
        List<Runnable> list = keyExecutor.shutdownNow();
        assertEquals(90, list.size());
        keyExecutor.awaitTermination(1, TimeUnit.HOURS);
        long completedTaskCount = ((KeyThreadPoolExecutor) keyExecutor).getCompletedTaskCount();
        assertEquals(10, completedTaskCount);
    }

    @Test
    void test4() throws InterruptedException {
        ExecutorService keyExecutor = newKeyThreadPool(() -> 2,
                () -> new LinkedBlockingQueue<>(1), () -> 2,
               new KeyThreadPoolExecutor.DiscardOldestPolicy());

        CountDownLatch latch = new CountDownLatch(2);
        BlockingQueue<Runnable> queue = ((KeyThreadPoolExecutor) keyExecutor).getQueue();
        for (int i = 0; i < 101; i++) {
            final int k = i;
            keyExecutor.execute(new KeyRunnable() {
                @Override
                public long getKey() {
                    return k;
                }

                @Override
                public void run() {
                    latch.countDown();
                    try {
                        Thread.sleep(100000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            if (i == 1) {
                latch.await();
            }
            if (i <= 1) {
                continue;
            }
            assertNull(queue.poll());
            assertNull(queue.poll(2, TimeUnit.MILLISECONDS));
            if (i == 2) {
                assertEquals(1, queue.size());
                Runnable next = queue.iterator().next();
                assertEquals(2, ((KeySupplier) next).getKey());
            }
            if (i >= 3) {
                if (i % 2 == 1) {
                    checkQueue(queue, i-1, i);
                } else {
                    checkQueue(queue, i, i - 1);
                }
            }
        }
        List<Runnable> list = new ArrayList<>();
        queue.drainTo(list);
        assertEquals(2, list.size());
        keyExecutor.shutdownNow();
        keyExecutor.awaitTermination(1, TimeUnit.HOURS);
    }

    @Test
    void test5() throws InterruptedException {
        ExecutorService keyExecutor = newKeyThreadPool(() -> 1,
                () -> new LinkedBlockingQueue<>(), () -> 3);

        CountDownLatch latch = new CountDownLatch(1);
        BlockingQueue<Runnable> queue = ((KeyThreadPoolExecutor) keyExecutor).getQueue();
        for (int i = 0; i < 100; i++) {
            final int k = i;
            keyExecutor.execute(new KeyRunnable() {
                @Override
                public long getKey() {
                    return k;
                }

                @Override
                public void run() {
                    if (k == 0) {
                        latch.countDown();
                        try {
                            Thread.sleep(100000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
            if (i == 0) {
                latch.await();
            }
        }
        Runnable task1 = queue.poll(2, TimeUnit.MILLISECONDS);
        assertNotNull(task1);
        Runnable task2 = queue.poll();
        assertNotNull(task2);
        Runnable task3 = queue.poll();
        assertNull(task3);
        task3 = queue.poll(2, TimeUnit.MILLISECONDS);
        assertNull(task3);
        task1.run();
        task3 = queue.poll();
        assertNotNull(task3);
        Runnable task4 = queue.poll();
        assertNull(task4);
        task2.run();
        Runnable task5 = queue.take();
        assertNotNull(task5);
        task4 = queue.poll();
        assertNull(task4);
        task5.run();
        task4 = queue.poll();
        assertNotNull(task4);
        assertNull(queue.peek());
        task4.run();
        assertNotNull(queue.peek());
        task4 = queue.poll();
        assertNotNull(task4);

        assertTrue(queue.size() > 0);
        queue.clear();
        assertTrue(queue.size() == 0);

        keyExecutor.shutdownNow();
        keyExecutor.awaitTermination(1, TimeUnit.HOURS);
    }

    private void checkQueue(BlockingQueue<Runnable> queue, int i2, int i3) {
        assertEquals(2, queue.size());
        Iterator<Runnable> iterator = queue.iterator();
        Runnable next = iterator.next();
        assertEquals(i2, ((KeySupplier) next).getKey());
        next = iterator.next();
        assertEquals(i3, ((KeySupplier) next).getKey());
    }
}