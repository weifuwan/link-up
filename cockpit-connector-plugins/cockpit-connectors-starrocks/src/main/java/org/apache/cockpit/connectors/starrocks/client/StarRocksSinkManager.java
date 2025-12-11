package org.apache.cockpit.connectors.starrocks.client;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.starrocks.config.SinkConfig;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorException;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class StarRocksSinkManager {

    private final SinkConfig sinkConfig;
    private final List<byte[]> batchList;

    private final StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;
    private volatile boolean initialize;
    private volatile Exception flushException;
    private int batchRowCount = 0;
    private long batchBytesSize = 0;

    public StarRocksSinkManager(SinkConfig sinkConfig, TableSchema tableSchema) {
        this(sinkConfig, tableSchema, new StarRocksStreamLoadVisitor(sinkConfig, tableSchema));
    }

    StarRocksSinkManager(
            SinkConfig sinkConfig,
            TableSchema tableSchema,
            StarRocksStreamLoadVisitor streamLoadVisitor) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        starrocksStreamLoadVisitor = streamLoadVisitor;
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        initialize = true;
    }

    public synchronized void write(String record) throws IOException {
        tryInit();
        checkFlushException();
        byte[] bts = record.getBytes(StandardCharsets.UTF_8);
        batchList.add(bts);
        batchRowCount++;
        batchBytesSize += bts.length;
        if (batchRowCount >= sinkConfig.getBatchMaxSize()
                || batchBytesSize >= sinkConfig.getBatchMaxBytes()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        flush();
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        String label = createBatchLabel();
        StarRocksFlushTuple tuple =
                new StarRocksFlushTuple(label, batchBytesSize, new ArrayList<>(batchList));
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                Boolean successFlag = starrocksStreamLoadVisitor.doStreamLoad(tuple);
                if (successFlag) {
                    break;
                }
            } catch (Exception e) {
                log.warn("Writing records to StarRocks failed, retry times = {}", i, e);

                String labelAlreadyMessage =
                        String.format("Label [%s] has already been used", label);
                if (ExceptionUtils.getMessage(e).contains(labelAlreadyMessage)) {
                    log.warn("Label [{}] has already been used, Skipping this batch", label);
                    break;
                }
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.WRITE_RECORDS_FAILED,
                            "The number of retries was exceeded, writing records to StarRocks failed.",
                            e);
                }

                if (e instanceof StarRocksConnectorException
                        && ((StarRocksConnectorException) e).needReCreateLabel()) {
                    String newLabel = createBatchLabel();
                    log.warn(
                            String.format(
                                    "Batch label changed from [%s] to [%s]",
                                    tuple.getLabel(), newLabel));
                    tuple.setLabel(newLabel);
                }

                try {
                    long backoff =
                            Math.min(
                                    sinkConfig.getRetryBackoffMultiplierMs() * i,
                                    sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, e);
                }
            }
        }
        batchList.clear();
        batchRowCount = 0;
        batchBytesSize = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.FLUSH_DATA_FAILED, flushException);
        }
    }

    public String createBatchLabel() {
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(sinkConfig.getLabelPrefix())) {
            sb.append(sinkConfig.getLabelPrefix());
        }
        return sb.append(UUID.randomUUID()).toString();
    }
}
