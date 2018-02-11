package com.jiwei.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JvmAssertSwitchChecker {

    private final static Logger logger = LoggerFactory.getLogger(JvmAssertSwitchChecker.class);

    private static final String ASSERT_CHECK_MESSAGE = "断言未开启,请增加虚拟机参数: -ea";

    public static void run() {
        try {
            logger.info("准备检查JVM断言是否开启");

            assert false : "assert check";
            logger.error(ASSERT_CHECK_MESSAGE);
            System.out.println(ASSERT_CHECK_MESSAGE);

            System.exit(0);
        } catch (AssertionError e) {
            logger.info("JVM断言已开启");
        }
    }
}
