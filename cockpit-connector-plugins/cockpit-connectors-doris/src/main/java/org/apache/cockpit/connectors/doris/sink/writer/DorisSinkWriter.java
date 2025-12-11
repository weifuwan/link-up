package org.apache.cockpit.connectors.doris.sink.writer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.doris.config.DorisSinkConfig;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;
import org.apache.cockpit.connectors.doris.rest.models.RespContent;
import org.apache.cockpit.connectors.doris.serialize.DorisSerializer;
import org.apache.cockpit.connectors.doris.serialize.SeaTunnelRowSerializerFactory;
import org.apache.cockpit.connectors.doris.sink.LoadStatus;
import org.apache.cockpit.connectors.doris.util.HttpUtil;
import org.apache.cockpit.connectors.doris.util.UnsupportedTypeConverterUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class DorisSinkWriter
        implements SinkWriter<SeaTunnelRow> {
    private static final int INITIAL_DELAY = 200;
    private static final List<String> DORIS_SUCCESS_STATUS =
            new ArrayList<>(Arrays.asList(LoadStatus.SUCCESS, LoadStatus.PUBLISH_TIMEOUT));
    private long lastCheckpointId;
    private DorisStreamLoad dorisStreamLoad;
    private final DorisSinkConfig dorisSinkConfig;
    private final String labelPrefix;
    private final LabelGenerator labelGenerator;
    private final int intervalTime;
    private DorisSerializer serializer;
    private final CatalogTable catalogTable;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile Exception loadException = null;
    private TableSchema tableSchema;
    private final TablePath sinkTablePath;


    public DorisSinkWriter(
            SinkWriter.Context context,
            CatalogTable catalogTable,
            DorisSinkConfig dorisSinkConfig,
            String jobId) {
        this.dorisSinkConfig = dorisSinkConfig;
        this.catalogTable = catalogTable;
        log.info("restore checkpointId {}", lastCheckpointId);
        log.info("labelPrefix " + dorisSinkConfig.getLabelPrefix());
        this.labelPrefix =
                dorisSinkConfig.getLabelPrefix()
                        + "_"
                        + catalogTable.getTablePath().getFullName().replaceAll("\\.", "_")
                        + "_"
                        + jobId
                        + "_"
                        + 11;
        this.labelGenerator = new LabelGenerator(labelPrefix, dorisSinkConfig.getEnable2PC());
        this.scheduledExecutorService =
                new ScheduledThreadPoolExecutor(
                        1, new ThreadFactoryBuilder().setNameFormat("stream-load-check").build());
        this.intervalTime = dorisSinkConfig.getCheckInterval();
        this.tableSchema = catalogTable.getTableSchema();
        this.sinkTablePath = catalogTable.getTablePath();
        this.serializer = createSerializer(dorisSinkConfig, catalogTable.getSeaTunnelRowType());
        this.initializeLoad();
    }

    private void initializeLoad() {

        List<String> feNodes = Arrays.asList(dorisSinkConfig.getFrontends().split(","));
        Collections.shuffle(feNodes);
        int feNodesNum = feNodes.size();

        for (int i = 0; i < feNodesNum; i++) {
            try {
                log.info("Trying FE node {}  for stream load.", feNodes.get(i));
                this.dorisStreamLoad =
                        new DorisStreamLoad(
                                feNodes.get(i),
                                catalogTable.getTablePath(),
                                dorisSinkConfig,
                                labelGenerator,
                                new HttpUtil().getHttpClient());
                if (dorisSinkConfig.getEnable2PC()) {
                    dorisStreamLoad.abortPreCommit(labelPrefix, lastCheckpointId + 1);
                }
                break;
            } catch (Exception e) {
                if (i == feNodesNum - 1) {
                    log.error("All {} FE nodes failed, no more nodes to try", feNodesNum);
                    throw new DorisConnectorException(
                            DorisConnectorErrorCode.STREAM_LOAD_FAILED, e);
                }
                log.error(
                        "stream load error for feNode: {} with exception: {}",
                        feNodes.get(i),
                        e.getMessage());
            }
        }

        startLoad(labelGenerator.generateLabel(lastCheckpointId + 1));
        // when uploading data in streaming mode, we need to regularly detect whether there are
        // exceptions.
        scheduledExecutorService.scheduleWithFixedDelay(
                this::checkDone, INITIAL_DELAY, intervalTime, TimeUnit.MILLISECONDS);
    }



    @Override
    public void write(SeaTunnelRow element) throws IOException {
        checkLoadException();
        byte[] serialize =
                serializer.serialize(
                        dorisSinkConfig.isNeedsUnsupportedTypeCasting()
                                ? UnsupportedTypeConverterUtils.convertRow(element)
                                : element);
        if (Objects.isNull(serialize)) {
            return;
        }
        dorisStreamLoad.writeRecord(serialize);
        if (!dorisSinkConfig.getEnable2PC()
                && dorisStreamLoad.getRecordCount() >= dorisSinkConfig.getBatchSize()) {
            flush();
            startLoad(labelGenerator.generateLabel(lastCheckpointId));
        }
    }


    private RespContent flush() throws IOException {
        // disable exception checker before stop load.
        checkState(dorisStreamLoad != null);
        RespContent respContent = dorisStreamLoad.stopLoad();
        if (respContent != null && !DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
            String errMsg =
                    String.format(
                            "stream load error: %s, see more in %s",
                            respContent.getMessage(), respContent.getErrorURL());
            throw new DorisConnectorException(DorisConnectorErrorCode.STREAM_LOAD_FAILED, errMsg);
        }
        return respContent;
    }

    private void startLoad(String label) {
        this.dorisStreamLoad.startLoad(label);
    }


    private void checkDone() {
        // the load future is done and checked in prepareCommit().
        // this will check error while loading.
        String errorMsg;
        log.debug("start timer checker, interval {} ms", intervalTime);
        if ((errorMsg = dorisStreamLoad.getLoadFailedMsg()) != null) {
            log.error("stream load finished unexpectedly: {}", errorMsg);
            loadException =
                    new DorisConnectorException(
                            DorisConnectorErrorCode.STREAM_LOAD_FAILED, errorMsg);
        }
    }

    private void checkLoadException() {
        if (loadException != null) {
            throw new RuntimeException("error while loading data.", loadException);
        }
    }

    @Override
    public void close() throws IOException {
        if (!dorisSinkConfig.getEnable2PC()) {
            flush();
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (dorisStreamLoad != null) {
            dorisStreamLoad.close();
        }
    }

    private DorisSerializer createSerializer(
            DorisSinkConfig dorisSinkConfig, SeaTunnelRowType seaTunnelRowType) {
        return SeaTunnelRowSerializerFactory.createSerializer(dorisSinkConfig, seaTunnelRowType);
    }
}
