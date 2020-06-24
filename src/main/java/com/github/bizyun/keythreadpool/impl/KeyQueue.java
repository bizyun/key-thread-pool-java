package com.github.bizyun.keythreadpool.impl;

import javax.annotation.Nonnull;

import com.github.bizyun.keythreadpool.KeySupplier;

/**
 * @author zhangbiyun
 */
interface KeyQueue<E> {
    E poll(@Nonnull KeySupplier keySupplier);
}
