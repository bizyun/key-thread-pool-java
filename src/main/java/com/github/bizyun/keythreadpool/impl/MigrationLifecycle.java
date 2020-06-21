package com.github.bizyun.keythreadpool.impl;

/**
 * @author zhangbiyun
 */
interface MigrationLifecycle {

    int NORMAL = 0;
    int MIGRATING = 1;
    int MIGRATED = 2;

    void startMigrating();

    void stopMigrating();

    boolean isMigrating();

    boolean isMigrated();
}
