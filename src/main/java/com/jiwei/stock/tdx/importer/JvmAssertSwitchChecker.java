package com.jiwei.stock.tdx.importer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JvmAssertSwitchChecker {

    protected final static Logger log = LoggerFactory.getLogger(JvmAssertSwitchChecker.class);

    private static final String ASSERT_CHECK_MESSAGE = "断言未开启,请增加虚拟机参数: -ea";

    public static void run() {
        try {
            log.info("准备检查JVM断言是否开启");

            assert false : "assert check";
            log.error(ASSERT_CHECK_MESSAGE);
            System.out.println(ASSERT_CHECK_MESSAGE);

            System.exit(0);
        } catch (AssertionError e) {
            log.info("JVM断言已开启");
        }
    }
}
