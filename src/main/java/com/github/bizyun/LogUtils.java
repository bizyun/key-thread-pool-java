package com.github.bizyun;

import org.slf4j.Logger;

/**
 * @author zhangbiyun
 * @date 2020/6/21
 */
public final class LogUtils {

    public static void debugLog(Logger logger, String log, Object... args) {
        if (logger.isDebugEnabled()) {
            logger.debug(log, args);
        }
    }
}
