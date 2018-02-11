package com.jiwei.stock.tdx.importer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TdxImportMain {

    private static final Logger log = LoggerFactory.getLogger(TdxImportMain.class);

    private static final String PATH = "./data";

    private static java.util.Date tradeDay;

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

        log.info("dbUrl: {}", dbUrl);
        log.info("dbUser: {}", dbUser);
        log.info("impThreadCount: {}", impThreadCount);
        log.info("processThreadCount: {}", processThreadCount);

        assert impThreadCount >= 1 : "线程数不正确";
        assert processThreadCount >= 1 : "线程数不正确";

        File dataDir = new File(PATH);

        tradeDay = parseLastTradeDay(dataDir);

        if (args.length >= 1 && args[0].equals("currentDay")) {
            reimpQX(PATH);

            for (String fileName : dataDir.list()) {
                if (fileName.toUpperCase().equals("QX.TXT")) {
                    continue;
                }
                impCurrentDay(PATH + "/" + fileName);
            }
            calDaily();
        }
        else {
            importDirectory(dataDir);
        }
        getConnection().close();
        System.exit(0);
    }

    private static SimpleDateFormat getDateFormat() {
        return new SimpleDateFormat("yyyyMMdd");
    }

    private static java.util.Date parseLastTradeDay(File dir) throws Throwable {
        java.util.Date result = getDateFormat().parse("19990101");
        for (String fileName : getDataFiles(dir)) {
            java.util.Date date = parseLastTradeDay(dir, fileName);
            if (date.after(result)) {
                result = date;
            }
        }
        log.debug("最后一个交易日: {}", getDateFormat().format(result));;
        return result;
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

    private static java.util.Date parseLastTradeDay(File dir, String fileName) throws Throwable {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
        File file = new File(dir.getPath() + "/" + fileName);
        List<String> lines = FileUtils.readLines(file, "GBK");
        if (lines.size() <= 3) {
            return simpleDateFormat.parse("1999/01/01");
        }
        String line = lines.get(lines.size() - 2);
        String str = line.split(",")[0];
        java.util.Date result = simpleDateFormat.parse(str);
        return result;
    }

    private static void importDirectory(File dataDir) throws IOException, InterruptedException, Throwable {
        ExecutorService executorService = Executors.newFixedThreadPool(impThreadCount);

        List<Future> futures = new ArrayList<Future>();

        final File bakDir = new File(dataDir.getPath() + "/bak");
        if(!bakDir.exists()) {
            FileUtils.forceMkdir(bakDir);
        }
        for (final String fileName : getDataFiles(dataDir)) {
            log.info("准备导入: " + fileName);
            Future<?> future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.currentThread().setName(fileName);
                        log.info("开始导入");
                        File file = new File(PATH+ "/" + fileName);
                        importFile(file);
                        log.info("完成导入");
                        FileUtils.moveFileToDirectory(file, bakDir, false);
                    }
                    catch (Throwable e) {
                        log.error("导入失败: " + e, e);
                    }
                }
            });
            futures.add(future);
        }

        ThreadUtil.wait(futures);

        if (getDataFiles(dataDir).size() > 0) {
            log.error("文件导入未完成,请检查");
            System.exit(-1);
        }

        log.info("开始日处理reinit");
        getConnection().prepareCall("call init_shares()").execute();
        getConnection().commit();
        log.info("结束日处理reinit");

        futures.clear();
        executorService = Executors.newFixedThreadPool(processThreadCount);
        for (String fileName : getDataFiles(bakDir)) {
            final String shareCode = fileName.replace(".txt", "").substring(3);
            Future future = executorService.submit(new Runnable() {
                @Override
                public void run() {
                    Thread.currentThread().setName(shareCode);
                    recaluateShare(shareCode);
                }
            });
            futures.add(future);
        }
        ThreadUtil.wait(futures);
        log.debug("完成");
    }

    private static void recaluateShare(String code) {
        log.info("开始");
        try {
            CallableStatement stmt = getConnection().prepareCall("call cal_share(?)");
            stmt.setString(1, code);
            stmt.execute();
            getConnection().commit();
            log.info("处理成功");
        }
        catch (Throwable e) {
            log.error(e.getMessage(), e);
            log.info("发生错误，重新处理");
            recaluateShare(code);
        }
    }

    private static void calDaily() throws Throwable {
        log.info("开始日处理");
        getConnection().prepareCall("call cal_daily()").execute();
        getConnection().commit();
        log.info("结束日处理");
    }

    private static void reimpQX(String path) throws Throwable {
        log.info("权息处理QX.txt");
        List<String> lines = FileUtils.readLines(new File(path + "/QX.txt"), "GBK");
        for (String line : lines) {
            String code = line.substring(0, 6);
            String prefix = code.startsWith("6") ? "SH#" : "SZ#";
            String fileName = path + "/" + prefix + code + ".txt";
            importFile(new File(fileName));
        }
    }

    private static void impCurrentDay(String fileName) throws Throwable {
        log.info("导入当日数据: " + fileName);
        List<String> lines = FileUtils.readLines(new File(fileName), "GBK");
        String first = lines.get(0);
        int pos = first.indexOf("日线");

        String code = first.substring(0, 6);
        String name = first.substring(6, pos).trim();

        if (lines.size() <= 3) {
            log.info("未上市: code=" + code + ", name=" + name);
            return;
        }

        String line = lines.get(lines.size() - 2);
        LineInfo info = LineInfo.parseLine(code, name, line);

        deleteLine(info);

        if (info.getVolume() > 0 && info.getDate().equals(tradeDay)) {
            insertLine(info);

            PreparedStatement stmt = getConnection().prepareStatement("UPDATE Shares SET NAME = ? WHERE CODE = ?");
            stmt.setString(1, name);
            stmt.setString(2, code);
            stmt.executeUpdate();
            stmt.close();
        }
        else {
            log.info("停牌");
        }
    }

    private static void deleteLine(LineInfo info) throws Throwable {
        PreparedStatement stmt = getConnection().prepareStatement("DELETE Day_Trades WHERE code = ? AND TradeDay = ?");
        stmt.setString(1, info.getCode());
        stmt.setDate(2, new Date(info.getDate().getTime()));
        stmt.executeUpdate();
        stmt.close();
        getConnection().commit();
    }

    private static void importFile(File file) throws Throwable {
        log.info("导入所有数据: " + file.getName());
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
    }

    private static void deleteTradeRecords(String code) throws Throwable {
        PreparedStatement stmt = getConnection().prepareStatement("DELETE DAY_TRADES WHERE Code = ?");
        stmt.setString(1, code);
        stmt.executeUpdate();
        stmt.close();
        getConnection().commit();
    }

    private final static ThreadLocal<PreparedStatement> insertStmtThreadLocal = new ThreadLocal<PreparedStatement>();

    private static void insertLine(LineInfo info) throws Throwable {
        assert info.getVolume() > 0 : "股票已停牌";
        PreparedStatement insertStmt = null;
        if (insertStmtThreadLocal.get() == null) {
            insertStmt = getConnection().prepareStatement(
                    "INSERT INTO DAY_TRADES" + "    (TradeDay, Code, Name, OpenPx, HighPx, LowPx, Closepx, BasePx, Volume, Amount, Id) "
                            + "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1 + NVL((SELECT MAX(t.id) FROM day_trades t WHERE t.code = ?), 0) FROM Dual");
            insertStmtThreadLocal.set(insertStmt);
        }
        else {
            insertStmt = insertStmtThreadLocal.get();
        }
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

        getConnection().commit();
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
