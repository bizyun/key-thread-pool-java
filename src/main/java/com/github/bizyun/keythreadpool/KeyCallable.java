package com.github.bizyun.keythreadpool;

import java.util.concurrent.Callable;

/**
 * @author zhangbiyun
 * @date 2020/6/9
 */
public interface KeyCallable<V> extends Callable<V>, KeySupplier {
}
