package com.github.bizyun.keythreadpool.impl;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.reflect.FieldUtils;

import com.github.bizyun.keythreadpool.KeySupplier;

/**
 * @author zhangbiyun
 * @date 2020/6/13
 */
class KeySupplierScanner {

    private final Set<Object> scannedObjects = new HashSet<>();

    public KeySupplier scan(Object obj) {
        if (obj instanceof KeySupplier) {
            return (KeySupplier) obj;
        }

        for (Field field : FieldUtils.getAllFields(obj.getClass())) {
            KeySupplier keySupplier = doScan(obj, field, KeySupplier.class);
            if (keySupplier != null) {
                return keySupplier;
            }
            keySupplier = doScan(obj, field, Runnable.class);
            if (keySupplier != null) {
                return keySupplier;
            }
            keySupplier = doScan(obj, field, Callable.class);
            if (keySupplier != null) {
                return keySupplier;
            }
        }
        return null;
    }

    private <T> KeySupplier doScan(Object obj, Field field, Class<T> matchClass) {
        if (matchClass.isAssignableFrom(field.getType())) {
            boolean isAccessible = field.isAccessible();
            if (!isAccessible) {
                field.setAccessible(true);
            }
            try {
                T o = matchClass.cast(field.get(obj));
                if (!isAccessible) {
                    field.setAccessible(false);
                }
                if (o instanceof KeySupplier) {
                    return (KeySupplier) o;
                }
                if (!scannedObjects.contains(o)) {
                    scannedObjects.add(o);
                    return scan(o);
                }
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return null;
    }
}
