package com.github.bizyun.keythreadpool;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
public interface MigrationLifecycle {

    int NORMAL = 0;
    int MIGRATING = 1;
    int MIGRATED = 2;

    void startMigrating();

    void stopMigrating();

    boolean isMigrating();

    boolean isMigrated();
}
