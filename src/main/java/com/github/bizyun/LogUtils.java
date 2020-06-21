package com.github.bizyun;

import org.slf4j.Logger;

/**
 * @author zhangbiyun
 */
public final class LogUtils {

    public static void debugLog(Logger logger, String log, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(log, args);
        }
    }
}
