package com.github.bizyun.keythreadpool.impl;


import static com.github.bizyun.LogUtils.debugLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableSupplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Uninterruptibles;

/**
 * @author zhangbiyun
 */
class QueuePool<E> implements Iterable<BlockingQueueHolder<E>>, MigrationLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(QueuePool.class);
    private static final AtomicInteger POOL_ID = new AtomicInteger();

    private final int poolId;
    private final AtomicInteger state = new AtomicInteger(NORMAL);
    private final List<BlockingQueueHolder<E>> queueList;
    private final BlockingQueue<BlockingQueueHolder<E>> qq;

    public QueuePool(Supplier<BlockingQueue<E>> queueSupplier, int queueCount) {
        this.poolId = POOL_ID.getAndIncrement();
        List<BlockingQueueHolder<E>> qList = new ArrayList<>(queueCount);
        for (int i = 0; i < queueCount; i++) {
            qList.add(new BlockingQueueHolder<>(queueSupplier.get(), poolId, i));
        }
        this.queueList = ImmutableList.copyOf(qList);
        this.qq = new LinkedBlockingQueue<>();
    }

    public BlockingQueueHolder<E> selectQueue(long key) {
        return queueList.get((int) (key % queueList.size()));
    }

    public BlockingQueueHolder<E> bindQueueBlock() throws InterruptedException {
        return doBindQueue(() -> {
            BlockingQueueHolder<E> queue = qq.take();
            if (isMigrated()) {
                // producer may put element when migrated, so migrated queue maybe not empty
                assert queue.isMigrated();
                qq.offer(queue);
                debugLog(logger, "[bindQueue-take] pool-{} is migrated, {}", poolId, queue);
                throw new InterruptedException();
            }
            return queue;
        });
    }

    @Nullable
    public BlockingQueueHolder<E> bindQueue() {
        return doBindQueue(() -> {
            BlockingQueueHolder<E> queue = qq.poll();
            if (isMigrated()) {
                if (queue != null) {
                    assert queue.isMigrated();
                    qq.offer(queue);
                }
                debugLog(logger, "[bindQueue-poll] pool-{} is migrated, {}", poolId, queue);
                return null;
            }
            return queue;
        });
    }

    @Nullable
    public BlockingQueueHolder<E> bindQueue(long timeout, TimeUnit unit) throws InterruptedException {
        return doBindQueue(() -> {
            BlockingQueueHolder<E> queue = qq.poll(timeout, unit);
            if (isMigrated()) {
                if (queue != null) {
                    assert queue.isMigrated();
                    qq.offer(queue);
                }
                debugLog(logger, "[bindQueue-pollTimeout] pool-{} is migrated, {}", poolId,
                        queue);
                return null;
            }
            return queue;
        });
    }

    @Nullable
    public BlockingQueueHolder<E> bindQueueForMigrating() {
        assert isMigrating() || isMigrated();
        for (BlockingQueueHolder<E> queue : queueList) {
            if (queue.isMigrating() || queue.isMigrated()) {
                continue;
            }
            if (!queue.bind()) {
                debugLog(logger, "[bindQueueForMigrating] queue is bound, pool-{}, {}", poolId
                        , queue);
                continue;
            }
            return queue;
        }
        return null;
    }

    private <T extends Throwable> BlockingQueueHolder<E> doBindQueue(
            ThrowableSupplier<BlockingQueueHolder<E>, T> queueSupplier) throws T {
        outer:
        while (true) {
            BlockingQueueHolder<E> queue = queueSupplier.get();
            if (queue == null) {
                return null;
            }
            while (true) {
                if (!queue.bind()) {
                    continue outer;
                }
                if (isNotEmpty(queue)) {
                    return queue;
                } else {
                    queue.unbind();
                }
                if (!queue.getQueue().isEmpty()) {
                    continue;
                }
                continue outer;
            }
        }
    }

    private boolean isNotEmpty(BlockingQueueHolder<E> queue) {
        if (!queue.getQueue().isEmpty()) {
            return true;
        }
        queue.getIdle().set(true);
        if (!queue.getQueue().isEmpty()) {
            queue.getIdle().set(false);
            return true;
        }
        return false;
    }

    public E peek() {
        BlockingQueueHolder<E> queue = qq.peek();
        if (queue == null) {
            return null;
        }
        return queue.getQueue().peek();
    }

    public void unbindQueue(BlockingQueueHolder<E> queue) {
        putQueueUninterrupted(queue.unbind());
    }

    private void putQueueUninterrupted(BlockingQueueHolder<E> queue) {
        Uninterruptibles.putUninterruptibly(qq, queue);
    }

    public void markQueueNotIdle(BlockingQueueHolder<E> queue) {
        if (queue.getIdle().compareAndSet(true, false)) {
            putQueueUninterrupted(queue);
        }
    }

    public void clear() {
        for (BlockingQueueHolder<E> queue : queueList) {
            queue.getQueue().clear();
        }
    }

    public int remainingCapacity() {
        long count = 0;
        for (BlockingQueueHolder<E> queue : queueList) {
            count += queue.getQueue().remainingCapacity();
        }
        if (count > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) count;
    }

    public int drainTo(Collection<? super E> c) {
        int count = 0;
        for (BlockingQueueHolder<E> queue : queueList) {
            count += queue.getQueue().drainTo(c);
        }
        return count;
    }

    public int drainTo(Collection<? super E> c, int maxElements) {
        int count = 0;
        int left = maxElements;
        for (BlockingQueueHolder<E> queue : queueList) {
            count += queue.getQueue().drainTo(c, left);
            left = maxElements - count;
            if (left <= 0) {
                break;
            }
        }
        return count;
    }

    public Iterator<E> getElementIterator() {
        return Iterators.concat(queueList.stream()
                .map(BlockingQueueHolder::getQueue)
                .map(BlockingQueue::iterator).toArray(Iterator[]::new));
    }

    @Nonnull
    @Override
    public Iterator<BlockingQueueHolder<E>> iterator() {
        return queueList.iterator();
    }

    public int size() {
        return queueList.stream()
                .map(BlockingQueueHolder::getQueue)
                .mapToInt(BlockingQueue::size).sum();
    }

    public boolean allQueueMigrated() {
        return !queueList.stream().filter(queue -> !queue.isMigrated()).findAny().isPresent();
    }

    @Override
    public boolean isMigrating() {
        return state.get() == MIGRATING;
    }

    @Override
    public boolean isMigrated() {
        return state.get() == MIGRATED;
    }

    @Override
    public void startMigrating() {
        state.compareAndSet(NORMAL, MIGRATING);
    }

    @Override
    public void stopMigrating() {
        boolean result = state.compareAndSet(MIGRATING, MIGRATED);
        if (result) {
            qq.offer(queueList.get(0));
        }
    }

    public int getPoolId() {
        return poolId;
    }

    @Override
    public String toString() {
        return "QueuePool{" +
                "poolId=" + poolId +
                ", state=" + state +
                ", queueList.size=" + queueList.size() +
                ", qq.size=" + qq.size() +
                ", queueList=" + queueList +
                '}';
    }
}
