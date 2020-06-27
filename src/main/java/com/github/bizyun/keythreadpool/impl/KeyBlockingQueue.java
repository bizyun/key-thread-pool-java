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
import com.github.phantomthief.util.ThrowableFunction;
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
            if (queue.getQueue().offer(runnable, 1, TimeUnit.SECONDS)) {
                return true;
            }
            if (queue.isMigrating() || queue.isMigrated()) {
                return null;
            }
            // put-operation maybe blocked forever when old queue is migrated and no consumer take
            // from old queue
            queue.getMigrateLock().readLock().lock();
            try {
                if (!queue.isMigrated()) {
                    queue.getQueue().put(runnable);
                    return true;
                }
                return null;
            } finally {
                queue.getMigrateLock().readLock().unlock();
            }
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
            ThrowableFunction<BlockingQueueWrapper<Runnable>, Boolean, T> putQueueFunction) throws T {
        while (true) {
            isExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueWrapper<Runnable> queue = pool.selectQueue(getKey(runnable));
            Boolean result = putQueueFunction.apply(queue);
            if (result == null) {
                continue;
            }
            if (!result.booleanValue()) {
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

    private boolean removeFromQueue(Runnable runnable, BlockingQueueWrapper<Runnable> queue) {
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
        while (true) {
            tryExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueWrapper<Runnable> queue = pool.bindQueueBlock();
            if (isMigratedThenUnbind(pool, queue)) {
                continue;
            }
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
    }

    private boolean isMigratedThenUnbind(QueuePool<Runnable> pool, BlockingQueueWrapper<Runnable> queue) {
        assert !queue.isMigrating();
        if (queue.isMigrated()) {
            debugLog(logger, "[consumer] bind a migrated queue, just unbind to continue, {}",
                    queue);
            pool.unbindQueue(queue);
            return true;
        }
        return false;
    }

    @Nullable
    @Override
    public Runnable poll(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        while (true) {
            tryExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueWrapper<Runnable> queue = pool.bindQueue(timeout, unit);
            if (queue == null) {
                return null;
            }
            if (isMigratedThenUnbind(pool, queue)) {
                continue;
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
    }

    @Nullable
    @Override
    public Runnable poll() {
        while (true) {
            tryExpandOrShrink();
            QueuePool<Runnable> pool = queuePool();
            BlockingQueueWrapper<Runnable> queue = pool.bindQueue();
            if (queue == null) {
                return null;
            }
            if (isMigratedThenUnbind(pool, queue)) {
                continue;
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
                // some producer with readLock locked may blocked on put-operation, so migrate
                // first and then use writeLock tryLock
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
