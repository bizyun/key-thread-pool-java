package com.github.bizyun.keythreadpool.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import javax.annotation.Nonnull;

import com.github.bizyun.keythreadpool.KeySupplier;


/**
 * @author zhangbiyun
 */
class KeyFutureTask<V> extends FutureTask<V> implements KeySupplier {

    private final KeySupplier keySupplier;

    public KeyFutureTask(@Nonnull Callable<V> callable) {
        super(callable);
        if (callable instanceof KeySupplier) {
            this.keySupplier = (KeySupplier) callable;
        } else {
            this.keySupplier = scanKeySupplier(callable);
        }
    }

    public KeyFutureTask(@Nonnull Runnable runnable, V result) {
        super(runnable, result);
        if (runnable instanceof KeySupplier) {
            this.keySupplier = (KeySupplier) runnable;
        } else {
            this.keySupplier = scanKeySupplier(runnable);
        }
    }

    private KeySupplier scanKeySupplier(@Nonnull Object object) {
        KeySupplier keySupplier = new KeySupplierScanner().scan(object);
        if (keySupplier == null) {
            throw new IllegalStateException("not found keySupplier");
        }
        return keySupplier;
    }

    @Override
    public long getKey() {
        return keySupplier.getKey();
    }
}
