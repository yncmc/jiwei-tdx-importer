package com.jiwei.stock.tdx.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jiwei.utils.JvmAssertSwitchChecker;
import com.jiwei.utils.ThreadUtil;

public class TdxImportMain {

    private static final Logger logger = LoggerFactory.getLogger(TdxImportMain.class);

    private static final String PATH = "./data";

    private static String dbUrl;
    private static String dbUser;
    private static String dbPassword;

    private static int impThreadCount;
    private static int processThreadCount;

    /**
     * @param args
     * @throws Throwable
     */
    public static void main(String[] args) throws Throwable {
        JvmAssertSwitchChecker.run();

        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("config.properties")));
        dbUrl = properties.getProperty("db.url");
        dbUser = properties.getProperty("db.user");
        dbPassword = properties.getProperty("db.password");
        impThreadCount = Integer.parseInt(properties.getProperty("imp.threadCount"));
        processThreadCount = Integer.parseInt(properties.getProperty("process.threadCount"));

        logger.info("dbUrl: {}", dbUrl);
        logger.info("dbUser: {}", dbUser);
        logger.info("impThreadCount: {}", impThreadCount);
        logger.info("processThreadCount: {}", processThreadCount);

        assert impThreadCount >= 1 : "线程数不正确";
        assert processThreadCount >= 1 : "线程数不正确";

        File dataDir = new File(PATH);

        importDirectory(dataDir);
        getConnection().close();
        System.exit(0);
    }

    private static List<String> getDataFiles(File dir) {
        List<String> results = new ArrayList<String>();
        for (String fileName : dir.list()) {
            if(fileName.startsWith("SH#") || fileName.startsWith("SZ#")) {
                results.add(fileName);
            }
        }
        return results;
    }

    private static void importDirectory(File dataDir) throws IOException, InterruptedException, Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(impThreadCount);

        List<Future> futures = new ArrayList<Future>();

        final File bakDir = new File(dataDir.getPath() + "/bak");
        if(!bakDir.exists()) {
            FileUtils.forceMkdir(bakDir);
        }
        for (final String fileName : getDataFiles(dataDir)) {
            logger.info("准备导入: " + fileName);
            Future<?> future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.currentThread().setName(fileName);
                        logger.info("开始导入");
                        File file = new File(PATH+ "/" + fileName);
                        importFile(file);
                        logger.info("完成导入");
                        FileUtils.moveFileToDirectory(file, bakDir, false);
                    }
                    catch (Throwable e) {
                        logger.error("导入失败: " + e, e);
                    }
                }
            });
            futures.add(future);
        }

        ThreadUtil.wait(futures);

        if (getDataFiles(dataDir).size() > 0) {
            logger.error("文件导入未完成,请检查");
            System.exit(-1);
        }

        logger.info("开始日处理reinit");
        getConnection().prepareCall("call init_shares()").execute();
        getConnection().commit();
        logger.info("结束日处理reinit");

        futures.clear();
        executorService = Executors.newFixedThreadPool(processThreadCount);
        for (String fileName : getDataFiles(bakDir)) {
            final String shareCode = fileName.replace(".txt", "").substring(3);
            Future future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(shareCode);
                    recalcuateShare(shareCode);
                }
            });
            futures.add(future);
        }
        ThreadUtil.wait(futures);
        logger.debug("完成");
    }

    private static void recalcuateShare(String code) {
        logger.info("开始");
        try {
            CallableStatement stmt = prepareStatment("call cal_share(?)");
            stmt.setString(1, code);
            stmt.execute();
            getConnection().commit();
            logger.info("处理成功");
        }
        catch (Throwable e) {
            logger.error(e.getMessage(), e);
            logger.info("发生错误，重新处理");
            recalcuateShare(code);
        }
    }

    private static void importFile(File file) throws Throwable {
        logger.info("导入所有数据: " + file.getName());
        List<String> list = FileUtils.readLines(file, "GBK");
        String first = list.get(0);
        int pos = first.indexOf("日线");

        String code = first.substring(0, 6);
        String name = first.substring(6, pos).trim();

        deleteTradeRecords(code);

        for (int i = 2; i < list.size() - 1; i++) {
            String line = list.get(i);
            LineInfo info = LineInfo.parseLine(code, name, line);
            if (info.getVolume() > 0) {
                insertLine(info);
            }
        }
        getConnection().commit();
    }

    private static final String DELETE_SQL = "DELETE DAY_TRADES WHERE Code = ?";

    private static void deleteTradeRecords(String code) throws Throwable {
        PreparedStatement stmt = prepareStatment(DELETE_SQL);
        stmt.setString(1, code);
        stmt.executeUpdate();
    }

    private final static ThreadLocal<Map<String, PreparedStatement>> statements = new ThreadLocal<Map<String,PreparedStatement>>();

    private final static <T extends PreparedStatement> T prepareStatment(String sql) throws Throwable, Throwable {
        Map<String, PreparedStatement> map = statements.get();
        if (map == null) {
            map = new HashMap<String, PreparedStatement>();
            statements.set(map);
            logger.info("init map");
        }
        PreparedStatement stmt = map.get(sql);
        if (stmt == null) {
            logger.info("SQL: {}", sql);
            if(sql.toLowerCase().startsWith("call")) {
                stmt = getConnection().prepareCall(sql);
            }
            else {
                stmt = getConnection().prepareStatement(sql);
            }
            map.put(sql, stmt);
            logger.info("statement prepared!");
        }
        return (T) stmt;
    }

    private static final String INSERT_SQL =
            "INSERT INTO DAY_TRADES" +
            "    (TradeDay, Code, Name, OpenPx, HighPx, LowPx, Closepx, BasePx, Volume, Amount, Id) " +
            "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1 + NVL((SELECT MAX(t.id) FROM day_trades t WHERE t.code = ?), 0) FROM Dual";

    private static void insertLine(LineInfo info) throws Throwable {

        PreparedStatement insertStmt = prepareStatment(INSERT_SQL);

        insertStmt.setDate(1, new Date(info.getDate().getTime()));
        insertStmt.setString(2, info.getCode());
        insertStmt.setString(3, info.getName());
        insertStmt.setDouble(4, info.getOpenPx());
        insertStmt.setDouble(5, info.getHighPx());
        insertStmt.setDouble(6, info.getLowPx());
        insertStmt.setDouble(7, info.getClosePx());
        insertStmt.setDouble(8, 0);
        insertStmt.setLong(9, info.getVolume());
        insertStmt.setDouble(10, info.getAmount());
        insertStmt.setString(11, info.getCode());

        insertStmt.executeUpdate();
    }

    private final static ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<Connection>();

    private static Connection getConnection() throws Throwable {
        if (connectionThreadLocal.get() == null) {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            connectionThreadLocal.set(connection);
        }
        return connectionThreadLocal.get();
    }
}
