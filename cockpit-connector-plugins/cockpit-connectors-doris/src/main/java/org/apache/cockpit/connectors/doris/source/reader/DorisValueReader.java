package org.apache.cockpit.connectors.doris.source.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.source.ArrowToSeatunnelRowReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.doris.backend.BackendClient;
import org.apache.cockpit.connectors.doris.config.DorisSourceConfig;
import org.apache.cockpit.connectors.doris.config.DorisSourceOptions;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.cockpit.connectors.doris.exception.DorisConnectorException;
import org.apache.cockpit.connectors.doris.rest.PartitionDefinition;
import org.apache.cockpit.connectors.doris.rest.models.Schema;
import org.apache.cockpit.connectors.doris.source.DorisSourceTable;
import org.apache.cockpit.connectors.doris.source.serialization.Routing;
import org.apache.cockpit.connectors.doris.util.SchemaUtils;
import org.apache.doris.sdk.thrift.*;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.cockpit.connectors.doris.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;


@Slf4j
public class DorisValueReader {

    protected BackendClient client;
    protected Lock clientLock = new ReentrantLock();

    private PartitionDefinition partition;
    private DorisSourceTable dorisSourceTable;
    private DorisSourceConfig config;

    protected int offset = 0;
    protected AtomicBoolean eos = new AtomicBoolean(false);
    protected ArrowToSeatunnelRowReader rowBatch;

    // flag indicate if support deserialize Arrow to RowBatch asynchronously
    protected boolean deserializeArrowToRowBatchAsync;

    protected BlockingQueue<ArrowToSeatunnelRowReader> rowBatchBlockingQueue;
    private TScanOpenParams openParams;
    protected String contextId;
    protected Schema schema;

    protected SeaTunnelRowType seaTunnelRowType;
    protected boolean asyncThreadStarted;

    public DorisValueReader(
            PartitionDefinition partition,
            DorisSourceConfig config,
            DorisSourceTable dorisSourceTable) {
        this.partition = partition;
        this.config = config;
        this.dorisSourceTable = dorisSourceTable;
        this.client = backendClient();
        this.deserializeArrowToRowBatchAsync = config.getDeserializeArrowAsync();
        this.seaTunnelRowType = dorisSourceTable.getCatalogTable().getSeaTunnelRowType();
        int blockingQueueSize = config.getDeserializeQueueSize();
        if (this.deserializeArrowToRowBatchAsync) {
            this.rowBatchBlockingQueue = new ArrayBlockingQueue<>(blockingQueueSize);
        }
        init();
    }

    private void init() {
        clientLock.lock();
        try {
            this.openParams = openParams();
            TScanOpenResult openResult = this.client.openScanner(this.openParams);
            this.contextId = openResult.getContextId();
            this.schema = SchemaUtils.convertToSchema(openResult.getSelectedColumns());
        } finally {
            clientLock.unlock();
        }
        this.asyncThreadStarted = asyncThreadStarted();
        log.debug("Open scan result is, contextId: {}, schema: {}.", contextId, schema);
    }

    private BackendClient backendClient() {
        try {
            return new BackendClient(new Routing(partition.getBeAddress()), config);
        } catch (IllegalArgumentException e) {
            log.error("init backend:{} client failed,", partition.getBeAddress(), e);
            throw new DorisConnectorException(DorisConnectorErrorCode.BACKEND_CLIENT_FAILED, e);
        }
    }

    private TScanOpenParams openParams() {
        TScanOpenParams params = new TScanOpenParams();
        params.setCluster(DorisSourceOptions.DORIS_DEFAULT_CLUSTER);
        params.setDatabase(partition.getDatabase());
        params.setTable(partition.getTable());

        params.setTabletIds(Arrays.asList(partition.getTabletIds().toArray(new Long[]{})));
        params.setOpaquedQueryPlan(partition.getQueryPlan());
        // max row number of one read batch
        Integer batchSize = dorisSourceTable.getBatchSize();
        Integer queryDorisTimeout = config.getRequestQueryTimeoutS();
        Long execMemLimit = dorisSourceTable.getExecMemLimit();
        params.setBatchSize(batchSize);
        params.setQueryTimeout(queryDorisTimeout);
        params.setMemLimit(execMemLimit);
        params.setUser(config.getUsername());
        params.setPasswd(config.getPassword());
        log.debug(
                "Open scan params is,cluster:{},database:{},table:{},tabletId:{},batch size:{},query timeout:{},execution memory limit:{},user:{},query plan: {}",
                params.getCluster(),
                params.getDatabase(),
                params.getTable(),
                params.getTabletIds(),
                params.getBatchSize(),
                params.getQueryTimeout(),
                params.getMemLimit(),
                params.getUser(),
                params.getOpaquedQueryPlan());
        return params;
    }

    protected Thread asyncThread =
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            clientLock.lock();
                            try {
                                TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                                nextBatchParams.setContextId(contextId);
                                while (!eos.get()) {
                                    nextBatchParams.setOffset(offset);
                                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                                    eos.set(nextResult.isEos());
                                    if (!eos.get()) {
                                        ArrowToSeatunnelRowReader rowBatch =
                                                new ArrowToSeatunnelRowReader(
                                                        nextResult.getRows(),
                                                        seaTunnelRowType)
                                                        .readArrow();
                                        offset += rowBatch.getReadRowCount();
                                        rowBatch.close();
                                        try {
                                            rowBatchBlockingQueue.put(rowBatch);
                                        } catch (InterruptedException e) {
                                            throw new DorisConnectorException(
                                                    DorisConnectorErrorCode.ROW_BATCH_GET_FAILED,
                                                    e);
                                        }
                                    }
                                }
                            } finally {
                                clientLock.unlock();
                            }
                        }
                    });

    protected boolean asyncThreadStarted() {
        boolean started = false;
        if (deserializeArrowToRowBatchAsync) {
            asyncThread.start();
            started = true;
        }
        return started;
    }

    /**
     * read data and cached in rowBatch.
     *
     * @return true if hax next value
     */
    public boolean hasNext() {
        boolean hasNext = false;
        if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
            // support deserialize Arrow to RowBatch asynchronously
            if (rowBatch == null || !rowBatch.hasNext()) {
                while (!eos.get() || !rowBatchBlockingQueue.isEmpty()) {
                    if (!rowBatchBlockingQueue.isEmpty()) {
                        try {
                            rowBatch = rowBatchBlockingQueue.take();
                        } catch (InterruptedException e) {
                            throw new DorisConnectorException(
                                    DorisConnectorErrorCode.ROW_BATCH_GET_FAILED, e);
                        }
                        hasNext = true;
                        break;
                    } else {
                        // wait for rowBatch put in queue or eos change
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            } else {
                hasNext = true;
            }
        } else {
            clientLock.lock();
            try {
                // Arrow data was acquired synchronously during the iterative process
                if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
                    if (rowBatch != null) {
                        offset += rowBatch.getReadRowCount();
                        rowBatch.close();
                    }
                    TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
                    nextBatchParams.setContextId(contextId);
                    nextBatchParams.setOffset(offset);
                    TScanBatchResult nextResult = client.getNext(nextBatchParams);
                    eos.set(nextResult.isEos());
                    if (!eos.get()) {
                        rowBatch =
                                new ArrowToSeatunnelRowReader(
                                        nextResult.getRows(), seaTunnelRowType)
                                        .readArrow();
                    }
                }
                hasNext = !eos.get();
            } finally {
                clientLock.unlock();
            }
        }
        return hasNext;
    }

    /**
     * get next value.
     *
     * @return next value
     */
    public SeaTunnelRow next() {
        if (!hasNext()) {
            log.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new DorisConnectorException(
                    DorisConnectorErrorCode.SHOULD_NEVER_HAPPEN, "never happen error.");
        }
        SeaTunnelRow next = rowBatch.next();
        next.setTableId(dorisSourceTable.getTablePath().toString());
        return next;
    }

    public void close() {
        clientLock.lock();
        try {
            TScanCloseParams closeParams = new TScanCloseParams();
            closeParams.setContextId(contextId);
            client.closeScanner(closeParams);
        } catch (Exception e) {
            log.error("Failed to close reader with context id {}", contextId, e);
            throw new DorisConnectorException(DorisConnectorErrorCode.RESOURCE_CLOSE_FAILED, e);
        } finally {
            clientLock.unlock();
        }
    }
}
