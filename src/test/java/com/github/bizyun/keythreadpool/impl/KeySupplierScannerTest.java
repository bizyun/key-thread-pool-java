package com.github.bizyun.keythreadpool.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.github.bizyun.keythreadpool.KeySupplier;
import com.github.bizyun.keythreadpool.impl.testtask.TestKeyRunnable;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
class KeySupplierScannerTest {

    @Test
    void test1() {
        TestKeyRunnable testKeyRunner = new TestKeyRunnable(1, new AtomicInteger(), 1, new ConcurrentHashMap<>());
        Runnable r1 = () -> testKeyRunner.run();
        KeySupplier keySupplier = new KeySupplierScanner().scan(r1);
        assertNotNull(keySupplier);
        assertEquals(1, keySupplier.getKey());

        Runnable r2 = () -> r1.run();

        keySupplier = new KeySupplierScanner().scan(r2);
        assertNotNull(keySupplier);
        assertEquals(1, keySupplier.getKey());

        Callable<Void> c1 = () -> {
            r2.run();
            return null;
        };

        keySupplier = new KeySupplierScanner().scan(c1);
        assertNotNull(keySupplier);
        assertEquals(1, keySupplier.getKey());

        Runnable r3 = () -> {
            try {
                c1.call();
            } catch (Exception e) {
            }
        };
        keySupplier = new KeySupplierScanner().scan(r3);
        assertNotNull(keySupplier);
        assertEquals(1, keySupplier.getKey());

        Callable<Void> c2 = () -> c1.call();

        keySupplier = new KeySupplierScanner().scan(c2);
        assertNotNull(keySupplier);
        assertEquals(1, keySupplier.getKey());

    }

    @Test
    void test2() {
        TestRunnable t1 = new TestRunnable();
        TestRunnable t2 = new TestRunnable();
        t1.setR(t2);
        t2.setR(t1);

        KeySupplier keySupplier = new KeySupplierScanner().scan(t2);
        assertNull(keySupplier);
    }

    @Test
    void test3() {
        Runnable r1 = () -> new TestKeyRunnable(1, new AtomicInteger(), 1, new ConcurrentHashMap<>()).run();
        KeySupplier keySupplier = new KeySupplierScanner().scan(r1);
        assertNull(keySupplier);
    }

    private static class TestRunnable implements Runnable {
        private Runnable r;

        @Override
        public void run() {

        }

        public void setR(Runnable r) {
            this.r = r;
        }
    }

}