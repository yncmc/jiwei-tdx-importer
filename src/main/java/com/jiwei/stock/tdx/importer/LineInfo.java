package com.jiwei.stock.tdx.importer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LineInfo {

    private String code;
    private String name;
    private Date date;
    private double openPx;
    private double highPx;
    private double lowPx;
    private double closePx;
    private long volume;
    private double amount;

    public LineInfo(String code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public double getOpenPx() {
        return openPx;
    }

    public void setOpenPx(double openPx) {
        this.openPx = openPx;
    }

    public double getHighPx() {
        return highPx;
    }

    public void setHighPx(double highPx) {
        this.highPx = highPx;
    }

    public double getLowPx() {
        return lowPx;
    }

    public void setLowPx(double lowPx) {
        this.lowPx = lowPx;
    }

    public double getClosePx() {
        return closePx;
    }

    public void setClosePx(double closePx) {
        this.closePx = closePx;
    }

    public long getVolume() {
        return volume;
    }

    public void setVolume(long volumn) {
        volume = volumn;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "LineInfo [date=" + new SimpleDateFormat("yyyy/MM/dd").format(date) + ", openPx=" + openPx + ", highPx=" + highPx + ", lowPx="
                + lowPx + ", closePx=" + closePx + ", volumn=" + volume + ", amount=" + amount + "]";
    }

    static LineInfo parseLine(String code, String name, String line) throws ParseException {
        String[] ss = line.split(",");
        LineInfo info = new LineInfo(code, name);

        info.setDate(new SimpleDateFormat("yyyy/MM/dd").parse(ss[0]));
        info.setOpenPx(Double.parseDouble(ss[1]));
        info.setHighPx(Double.parseDouble(ss[2]));
        info.setLowPx(Double.parseDouble(ss[3]));
        info.setClosePx(Double.parseDouble(ss[4]));
        info.setVolume(Long.parseLong(ss[5]));
        info.setAmount(Double.parseDouble(ss[6]));

        return info;
    }
}
