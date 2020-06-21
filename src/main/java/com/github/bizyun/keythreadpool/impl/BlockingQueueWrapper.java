package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.LogUtils.debugLog;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.bizyun.keythreadpool.MigrationLifecycle;

/**
 * @author zhangbiyun
 * @date 2020/6/11
 */
class BlockingQueueWrapper<E> implements MigrationLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueWrapper.class);

    private final int poolId;
    private final int queueId;
    private final BlockingQueueProxy<E> queue;
    private final AtomicBoolean idle = new AtomicBoolean(true);
    private final AtomicReference<Thread> bindThread = new AtomicReference<>();
    private final ReadWriteLock migrateLock = new ReentrantReadWriteLock();
    private final AtomicInteger state = new AtomicInteger(NORMAL);

    public BlockingQueueWrapper(BlockingQueue<E> queue, int poolId, int queueId) {
        this.poolId = poolId;
        this.queueId = queueId;
        this.queue = new BlockingQueueProxy<>(queue);
    }

    public BlockingQueue<E> getQueue() {
        return queue;
    }

    boolean bind() {
        boolean result = bindThread.compareAndSet(null, Thread.currentThread());
        debugLog(logger, "[bind-queue], {}, {}", result, this);
        return result;
    }

    BlockingQueueWrapper<E> unbind() {
        boolean result = bindThread.compareAndSet(Thread.currentThread(), null);
        assert result;
        debugLog(logger, "[unbind-queue], {}", this);
        return this;
    }

    public AtomicBoolean getIdle() {
        return idle;
    }

    public ReadWriteLock getMigrateLock() {
        return migrateLock;
    }

    @Override
    public String toString() {
        Thread thread = bindThread.get();
        return "BlockingQueueWrapper{" +
                "poolId=" + poolId +
                ", queueId=" + queueId +
                ", queue.size=" + queue.size() +
                ", state=" + state +
                ", idle=" + idle.get() +
                ", bindThread=" + (thread == null ? "null" : thread.getName()) +
                '}';
    }

    @Override
    public void startMigrating() {
        state.compareAndSet(NORMAL, MIGRATING);
    }

    @Override
    public void stopMigrating() {
        state.compareAndSet(MIGRATING, MIGRATED);
    }

    @Override
    public boolean isMigrating() {
        return state.get() == MIGRATING;
    }

    @Override
    public boolean isMigrated() {
        return state.get() == MIGRATED;
    }
}
