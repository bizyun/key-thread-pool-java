package com.github.bizyun.keythreadpool.impl;

import static com.github.bizyun.LogUtils.debugLog;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangbiyun
 */
class BlockingQueueHolder<E> implements MigrationLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(BlockingQueueHolder.class);

    private final int poolId;
    private final int queueId;
    private final BlockingQueueProxy<E> queue;
    private final AtomicBoolean idle = new AtomicBoolean(true);
    private final AtomicReference<Thread> bindThread = new AtomicReference<>();
    private final AtomicInteger state = new AtomicInteger(NORMAL);

    public BlockingQueueHolder(BlockingQueue<E> queue, int poolId, int queueId) {
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

    BlockingQueueHolder<E> unbind() {
        boolean result = bindThread.compareAndSet(Thread.currentThread(), null);
        assert result;
        debugLog(logger, "[unbind-queue], {}", this);
        return this;
    }

    public AtomicBoolean getIdle() {
        return idle;
    }

    @Override
    public String toString() {
        Thread thread = bindThread.get();
        return "BlockingQueueHolder{" +
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
