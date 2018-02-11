package com.jiwei.stock.tdx.importer;

import java.util.List;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadUtil {

    private static final Logger log = LoggerFactory.getLogger(ThreadUtil.class);

    public static void wait(List<Future> futures) {
        int i = 1;
        while (i > 0) {
            i = 0;
            for (Future<?> future : futures) {
                if (!future.isDone()) {
                    i++;
                }
            }
            log.info("等待完成: " + i);
            sleep(2000);
        }
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
