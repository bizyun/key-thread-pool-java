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
import com.github.phantomthief.util.ThrowablePredicate;
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
        addAndMark(runnable, queue -> {
            queue.getQueue().put(runnable);
            return true;
        });
    }

    @Override
    public boolean offer(Runnable runnable, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return addAndMark(runnable, queue -> queue.getQueue().offer(runnable, timeout, unit));
    }

    @Override
    public boolean offer(Runnable runnable) {
        return addAndMark(runnable, queue -> queue.getQueue().offer(runnable));
    }


    private <T extends Throwable> boolean addAndMark(Runnable runnable,
            ThrowablePredicate<BlockingQueueHolder<Runnable>, T> predicate) throws T {
        while (true) {
            isExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueHolder<Runnable> queue = pool.selectQueue(getKey(runnable));
            if (!predicate.test(queue)) {
                return false;
            }
            if (queue.isMigrating() || queue.isMigrated()) {
                if (removeFromQueue(runnable, queue)) {
                    continue;
                }
            }
            pool.markQueueNotIdle(queue);
            return true;
        }
    }

    private boolean removeFromQueue(Runnable runnable, BlockingQueueHolder<Runnable> queue) {
        if (queue.getQueue().remove(runnable)) {
            debugLog(logger, "[producer] put to a migrating or migrated queue, remove success, {}",
                    queue);
            return true;
        }
        debugLog(logger, "[producer] put to a migrating or migrated queue, remove failed，maybe "
                        + "migrated to new queue, {}", queue);
        return false;
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
        BlockingQueueHolder<Runnable> queue = pool.bindQueueBlock();
        assert !queue.isMigrating() && !queue.isMigrated();
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
        BlockingQueueHolder<Runnable> queue = pool.bindQueue(timeout, unit);
        if (queue == null) {
            return null;
        }
        assert !queue.isMigrating() && !queue.isMigrated();
        Runnable runnable;
        try {
            runnable = queue.getQueue().poll(timeout, unit);
            assert runnable != null;
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
    public Runnable poll() {
        tryExpandOrShrink();
        QueuePool<Runnable> pool = queuePool();
        BlockingQueueHolder<Runnable> queue = pool.bindQueue();
        if (queue == null) {
            return null;
        }
        assert !queue.isMigrating() && !queue.isMigrated();
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
            BlockingQueueHolder<Runnable> queue = pool.bindQueueForMigrating();
            if (queue != null) {
                migrateOneQueue(queue);
                pool.increaseMigratedQueueCount();
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
                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
            }
        }
    }

    private void migrateOneQueue(BlockingQueueHolder<Runnable> queue) {
        QueuePool<Runnable> backupPool = this.backupQueuePoolSupplier.get();
        queue.startMigrating();
        debugLog(logger, "[migrateOneQueue] {}", queue);
        for (Runnable r; (r = queue.getQueue().poll()) != null; ) {
            BlockingQueueHolder<Runnable> backupQueue = backupPool.selectQueue(getKey(r));
            if (!backupQueue.getQueue().offer(r)) {
                debugLog(logger, "[migrateOneQueue] queue is full, {}", queue);
                continue;
            }
            backupPool.markQueueNotIdle(backupQueue);
//            debugLog(logger, "[migrateOneQueue] backupQueue:{}", backupQueue);
        }
        debugLog(logger, "[migrateOneQueue] complete {}", queue);
        queue.stopMigrating();
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
            BlockingQueueHolder<Runnable> queue = queuePool().selectQueue(((KeySupplier) o).getKey());
            return queue.getQueue().remove(o);
        }
        return false;
    }

    private long getKey(Runnable runnable) {
        return ((KeySupplier) runnable).getKey();
    }

    @Override
    public Runnable poll(@Nonnull KeySupplier keySupplier) {
        BlockingQueueHolder<Runnable> queue = queuePool().selectQueue(keySupplier.getKey());
        return queue.getQueue().poll();
    }
}
