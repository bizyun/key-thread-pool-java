package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.LogUtils.debugLog;
import static com.google.common.base.Suppliers.memoize;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.bizyun.keythreadpool.KeySupplier;
import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author zhangbiyun
 */
class KeyBlockingQueue extends AbstractQueue<Runnable> implements BlockingQueue<Runnable>,
        KeyQueue<Runnable> {

    private static final Logger logger = LoggerFactory.getLogger(KeyBlockingQueue.class);

    private final Supplier<BlockingQueue<Runnable>> queueSupplier;
    private final IntSupplier queueCountSupplier;
    private volatile int curQueueCount;
    private volatile Supplier<QueuePool<Runnable>> queuePoolSupplier;

    private final AtomicBoolean expandOrShrink = new AtomicBoolean();
    private volatile CountDownLatch expandOrShrinkProducerLock;
    private volatile Supplier<QueuePool<Runnable>> backupQueuePoolSupplier;

    public KeyBlockingQueue(Supplier<BlockingQueue<Runnable>> queueSupplier,
            IntSupplier queueCountSupplier) {
        this.curQueueCount = queueCountSupplier.getAsInt();
        Preconditions.checkArgument(this.curQueueCount > 0, "queue count <= 0");
        this.queueCountSupplier = queueCountSupplier;
        this.queueSupplier = queueSupplier;
        this.queuePoolSupplier = memoize(() -> new QueuePool<>(queueSupplier, this.curQueueCount));
    }

    @Override
    public Iterator<Runnable> iterator() {
        return queuePool().getElementIterator();
    }

    @Override
    public int size() {
        return queuePool().size();
    }

    @Override
    public void put(@Nonnull Runnable runnable) throws InterruptedException {
        addAndMark(runnable, queue -> queue.getQueue().put(runnable));
    }

    @Override
    public boolean offer(Runnable runnable, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        AtomicBoolean result = new AtomicBoolean();
        addAndMark(runnable, queue -> result.set(queue.getQueue().offer(runnable, timeout, unit)));
        return result.get();
    }

    @Override
    public boolean offer(Runnable runnable) {
        AtomicBoolean result = new AtomicBoolean();
        addAndMark(runnable, queue -> result.set(queue.getQueue().offer(runnable)));
        return result.get();
    }


    private <T extends Throwable> void addAndMark(Runnable runnable,
            ThrowableConsumer<BlockingQueueWrapper<Runnable>, T> consumer) throws T {
        while (true) {
            isExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueWrapper<Runnable> queue = pool.selectQueue(getKey(runnable));
            queue.getMigrateLock().readLock().lock();
            try {
                if (!queue.isMigrated()) {
                    consumer.accept(queue);
                    pool.markQueueNotIdle(queue);
                    return;
                } else {
                    debugLog(logger, "[addAndMark] queue is migrated, {}", queue);
                }
            } finally {
                queue.getMigrateLock().readLock().unlock();
            }
        }
    }

    private void isExpandOrShrink() {
        if (expandOrShrink.get()) {
            CountDownLatch latch = expandOrShrinkProducerLock;
            if (latch != null) {
                debugLog(logger, "[expandOrShrink] producer await");
                Uninterruptibles.awaitUninterruptibly(latch);
                debugLog(logger, "[expandOrShrink] producer wakeup");
            }
        }
    }

    private QueuePool<Runnable> queuePool() {
        return queuePoolSupplier.get();
    }

    @Nonnull
    @Override
    public Runnable take() throws InterruptedException {
        tryExpandOrShrink();
        QueuePool<Runnable> pool = queuePool();
        BlockingQueueWrapper<Runnable> queue = pool.bindQueueBlock();
        Runnable runnable;
        try {
            runnable = queue.getQueue().take();
        } catch (InterruptedException e) {
            pool.unbindQueue(queue);
            throw e;
        }
        return () -> {
            try {
                runnable.run();
            } finally {
                pool.unbindQueue(queue);
            }
        };
    }

    @Nullable
    @Override
    public Runnable poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        tryExpandOrShrink();
        QueuePool<Runnable> pool = queuePool();
        BlockingQueueWrapper<Runnable> queue = pool.bindQueue(timeout, unit);
        if (queue == null) {
            return null;
        }
        Runnable runnable = queue.getQueue().poll(timeout, unit);
        assert runnable != null;
        return () -> {
            try {
                runnable.run();
            } finally {
                pool.unbindQueue(queue);
            }
        };
    }

    @Nullable
    @Override
    public Runnable poll() {
        tryExpandOrShrink();
        QueuePool<Runnable> pool = queuePool();
        BlockingQueueWrapper<Runnable> queue = pool.bindQueue();
        if (queue == null) {
            return null;
        }
        Runnable runnable = queue.getQueue().poll();
        assert runnable != null;
        return () -> {
            try {
                runnable.run();
            } finally {
                pool.unbindQueue(queue);
            }
        };
    }

    private void tryExpandOrShrink() {
        QueuePool<Runnable> pool = queuePool();
        int queueCount = this.queueCountSupplier.getAsInt();
        if (queueCount > 0 && queueCount != this.curQueueCount && expandOrShrink.compareAndSet(false, true)) {
            debugLog(logger, "[expandOrShrink] start, oldQueueCount:{}, newQueueCount:{}, " +
                    "pool:{}", this.curQueueCount, queueCount, pool);
            this.curQueueCount = queueCount;
            this.backupQueuePoolSupplier = memoize(() -> new QueuePool<>(queueSupplier, this.curQueueCount));
            this.expandOrShrinkProducerLock = new CountDownLatch(1);
            pool.startMigrating();
        }

        if (pool.isMigrating()) {
            doMigrate(pool);
        }
    }

    private void doMigrate(QueuePool<Runnable> pool) {
        while (true) {
            BlockingQueueWrapper<Runnable> queue = pool.bindQueueForMigrating();
            if (queue != null) {
                try {
                    migrateOneQueue(queue);
                } finally {
                    pool.unbindQueue(queue);
                }
            } else if (pool.allQueueMigrated()) {
                synchronized (expandOrShrink) {
                    if (!pool.isMigrated()) {
                        this.queuePoolSupplier = this.backupQueuePoolSupplier;
                        pool.stopMigrating();
                        this.expandOrShrinkProducerLock.countDown();
                        this.backupQueuePoolSupplier = null;
                        this.expandOrShrinkProducerLock = null;
                        this.expandOrShrink.set(false);
                        debugLog(logger, "[expandOrShrink] complete, oldPool:{}, " +
                                "newPool:{}", pool, queuePool());
                    } else {
                        debugLog(logger, "[expandOrShrink] complete, oldPoolId:{}, " +
                                "newPoolId:{}", pool.getPoolId(), queuePool().getPoolId());
                    }
                    return;
                }
            } else {
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void migrateOneQueue(BlockingQueueWrapper<Runnable> queue) {
        QueuePool<Runnable> backupPool = this.backupQueuePoolSupplier.get();
        boolean interrupted = false;

        queue.startMigrating();
        try {
            while (true) {
                debugLog(logger, "[migrateOneQueue] {}", queue);
                doMigrateOneQueue(queue, backupPool);
                try {
                    if (queue.getMigrateLock().writeLock().tryLock(5, TimeUnit.MILLISECONDS)) {
                        try {
                            doMigrateOneQueue(queue, backupPool);
                            queue.stopMigrating();
                            debugLog(logger, "[migrateOneQueue] complete, {}", queue);
                            return;
                        } finally {
                            queue.getMigrateLock().writeLock().unlock();
                        }
                    } else {
                        debugLog(logger, "[migrateOneQueue] writeLock.tryLock failed, {}",
                                queue);
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void doMigrateOneQueue(BlockingQueueWrapper<Runnable> queue, QueuePool<Runnable> backupPool) {
        for (Runnable r; (r = queue.getQueue().poll()) != null; ) {
            BlockingQueueWrapper<Runnable> backupQueue = backupPool.selectQueue(getKey(r));
            if (!backupQueue.getQueue().offer(r)) {
                debugLog(logger, "[migrateOneQueue] queue is full, {}", queue);
                continue;
            }
            backupPool.markQueueNotIdle(backupQueue);
//            debugLog(logger, "[migrateOneQueue] backupQueue:{}", backupQueue);
        }
    }

    @Override
    public Runnable peek() {
        return queuePool().peek();
    }

    @Override
    public void clear() {
        queuePool().clear();
    }

    @Override
    public int remainingCapacity() {
        return queuePool().remainingCapacity();
    }

    @Override
    public int drainTo(@Nonnull Collection<? super Runnable> c) {
        return queuePool().drainTo(c);
    }

    @Override
    public int drainTo(@Nonnull Collection<? super Runnable> c, int maxElements) {
        return queuePool().drainTo(c, maxElements);
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof KeySupplier) {
            BlockingQueueWrapper<Runnable> queue = queuePool().selectQueue(((KeySupplier) o).getKey());
            return queue.getQueue().remove(o);
        }
        return false;
    }

    private long getKey(Runnable runnable) {
        return ((KeySupplier) runnable).getKey();
    }

    @Override
    public Runnable poll(@Nonnull KeySupplier keySupplier) {
        BlockingQueueWrapper<Runnable> queue = queuePool().selectQueue(keySupplier.getKey());
        return queue.getQueue().poll();
    }
}
