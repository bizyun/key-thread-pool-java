package com.github.bizyun.keythreadpool.impl;

import java.util.concurrent.BlockingQueue;

import com.google.common.util.concurrent.ForwardingBlockingQueue;

/**
 * @author zhangbiyun
 */
class BlockingQueueProxy<E> extends ForwardingBlockingQueue<E> {

    private final BlockingQueue<E> queue;

    public BlockingQueueProxy(BlockingQueue<E> queue) {
        this.queue = queue;
    }

    @Override
    protected BlockingQueue<E> delegate() {
        return queue;
    }
}
