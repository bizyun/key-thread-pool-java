package com.github.bizyun.keythreadpool.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import com.github.bizyun.keythreadpool.KeyRunnable;
import com.github.bizyun.keythreadpool.KeySupplier;
import com.google.common.base.Preconditions;

/**
 * @author zhangbiyun
 */
public class KeyThreadPoolExecutor extends ThreadPoolExecutor {

    private final IntSupplier poolSizeSupplier;
    private volatile boolean hasStartCoreThreads = false;

    public static KeyThreadPoolExecutor newKeyThreadPool(IntSupplier poolSizeSupplier,
            Supplier<BlockingQueue<Runnable>> queueSupplier, IntSupplier queueCountSupplier) {
        return new KeyThreadPoolExecutor(poolSizeSupplier.getAsInt(),
                poolSizeSupplier,
                Executors.defaultThreadFactory(),
                new CallerBlockingPolicy(),
                queueSupplier,
                queueCountSupplier);
    }

    public static KeyThreadPoolExecutor newKeyThreadPool(IntSupplier poolSizeSupplier,
            ThreadFactory threadFactory, Supplier<BlockingQueue<Runnable>> queueSupplier,
            IntSupplier queueCountSupplier) {
        return new KeyThreadPoolExecutor(poolSizeSupplier.getAsInt(),
                poolSizeSupplier,
                threadFactory,
                new CallerBlockingPolicy(),
                queueSupplier,
                queueCountSupplier);
    }

    public static KeyThreadPoolExecutor newKeyThreadPool(IntSupplier poolSizeSupplier,
            Supplier<BlockingQueue<Runnable>> queueSupplier, IntSupplier queueCountSupplier,
            RejectedExecutionHandler handler) {
        return new KeyThreadPoolExecutor(poolSizeSupplier.getAsInt(),
                poolSizeSupplier,
                Executors.defaultThreadFactory(),
                handler,
                queueSupplier,
                queueCountSupplier);
    }

    public static KeyThreadPoolExecutor newKeyThreadPool(IntSupplier poolSizeSupplier,
            ThreadFactory threadFactory, Supplier<BlockingQueue<Runnable>> queueSupplier,
            IntSupplier queueCountSupplier, RejectedExecutionHandler handler) {
        return new KeyThreadPoolExecutor(poolSizeSupplier.getAsInt(),
                poolSizeSupplier,
                threadFactory,
                handler,
                queueSupplier,
                queueCountSupplier);
    }


    private KeyThreadPoolExecutor(int poolSize, IntSupplier poolSizeSupplier,
            ThreadFactory threadFactory, RejectedExecutionHandler handler,
            Supplier<BlockingQueue<Runnable>> queueSupplier, IntSupplier queueCountSupplier) {
        super(poolSize, poolSize, 0, TimeUnit.MILLISECONDS,
                new KeyBlockingQueue(queueSupplier, queueCountSupplier), threadFactory, handler);
        this.poolSizeSupplier = poolSizeSupplier;
        checkNotSupportPolicy(handler);
    }

    private void checkNotSupportPolicy(RejectedExecutionHandler handler) {
        Preconditions.checkArgument(!(handler instanceof CallerRunsPolicy));
        Preconditions.checkArgument(!(handler instanceof ThreadPoolExecutor.DiscardOldestPolicy));
    }

    @Override
    public void execute(Runnable command) {
        Preconditions.checkNotNull(command);

        if (!(command instanceof KeySupplier)) {
            command = new KeyRunnableWrapper(command);
        }

        if (!hasStartCoreThreads) {
            hasStartCoreThreads = true;
            prestartAllCoreThreads();
        }
        if (!isShutdown() && getQueue().offer(command)) {
            tryChangePoolSize();
            if (isShutdown() && getQueue().remove(command)) {
                getRejectedExecutionHandler().rejectedExecution(command, this);
            }
        } else {
            getRejectedExecutionHandler().rejectedExecution(command, this);
        }
    }

    private void tryChangePoolSize() {
        int poolSize = poolSizeSupplier.getAsInt();
        if (poolSize <= 0) {
            return;
        }
        if (getCorePoolSize() != poolSize) {
            if (poolSize > getCorePoolSize()) {
                setMaximumPoolSize(poolSize);
                setCorePoolSize(poolSize);
                prestartAllCoreThreads();
            } else {
                setCorePoolSize(poolSize);
                setMaximumPoolSize(poolSize);
            }
        }
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new KeyFutureTask<>(runnable, value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new KeyFutureTask<>(callable);
    }

    private static class KeyRunnableWrapper implements KeyRunnable {

        private final Runnable original;
        private final KeySupplier keySupplier;

        public KeyRunnableWrapper(Runnable original) {
            this.original = original;
            this.keySupplier = new KeySupplierScanner().scan(original);
            if (this.keySupplier == null) {
                throw new IllegalStateException("not found keySupplier");
            }
        }

        @Override
        public void run() {
            original.run();
        }

        @Override
        public long getKey() {
            return keySupplier.getKey();
        }
    }

    public static class CallerBlockingPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            BlockingQueue<Runnable> queue = executor.getQueue();
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        if (executor.isShutdown()) {
                            throw new RejectedExecutionException("executor is shutdown");
                        }
                        queue.put(r);
                        break;
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
    }

    public static class DiscardOldestPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            if (!executor.isShutdown()) {
                BlockingQueue<Runnable> queue = executor.getQueue();
                if (queue instanceof KeyBlockingQueue && r instanceof KeySupplier) {
                    ((KeyBlockingQueue) queue).removeOldest((KeySupplier) r);
                    executor.execute(r);
                } else {
                    throw new IllegalStateException("is not KeyThreadPoolExecutor");
                }
            }
        }
    }

}
