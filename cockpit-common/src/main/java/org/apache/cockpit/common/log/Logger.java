package org.apache.cockpit.common.log;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.utils.DateTimeUtils;

import java.io.*;

/**
 * 日志输出工具
 */
@Slf4j
public class Logger  {

    private OutputStream outStream;
    private PrintStream printStream;

    public Logger(String logFilePath) throws FileNotFoundException {
        this.outStream = new FileOutputStream(logFilePath);
        this.printStream = new PrintStream(this.outStream, true);
    }

    /**
     * 关闭工具，关闭对象关联的文件输出流
     */
    public synchronized void close() {
        try {
            if (outStream != null) {
                printStream.close();
                outStream.close();
            }
            printStream = null;
            outStream = null;
        } catch (IOException e) {
            log.error("", e);
            throw new IllegalStateException("IOException when close", e);
        }
    }

    /**
     * 写入一行字符串到构建日志文件
     */
    public void log(String str) {
        log.info(str);
        printStream.println(str);
    }


    /**
     * 生成默认的文件名
     */
    private static String generateLogDefaultFileName() {
        return "sync_" + DateTimeUtils.formatMillisDefault() + ".log";
    }
}
