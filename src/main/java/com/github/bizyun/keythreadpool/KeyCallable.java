package com.github.bizyun.keythreadpool;

import java.util.concurrent.Callable;

/**
 * @author zhangbiyun
 */
public interface KeyCallable<V> extends Callable<V>, KeySupplier {
}
