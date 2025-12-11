package org.apache.cockpit.connectors.starrocks.client.source;

import com.starrocks.shade.org.apache.thrift.TException;
import com.starrocks.shade.org.apache.thrift.protocol.TBinaryProtocol;
import com.starrocks.shade.org.apache.thrift.protocol.TProtocol;
import com.starrocks.shade.org.apache.thrift.transport.TSocket;
import com.starrocks.shade.org.apache.thrift.transport.TTransportException;
import com.starrocks.thrift.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;
import org.apache.cockpit.connectors.api.source.ArrowToSeatunnelRowReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.starrocks.client.source.model.QueryPartition;
import org.apache.cockpit.connectors.starrocks.config.SourceConfig;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorErrorCode;
import org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.cockpit.connectors.starrocks.exception.StarRocksConnectorErrorCode.CLOSE_BE_READER_FAILED;


@Slf4j
public class StarRocksBeReadClient implements Serializable {
    private static final String DEFAULT_CLUSTER_NAME = "default_cluster";

    private TStarrocksExternalService.Client client;
    private final String ip;
    private final int port;
    private String contextId;
    private int readerOffset = 0;
    private final SourceConfig sourceConfig;
    private SeaTunnelRowType seaTunnelRowType;
    private ArrowToSeatunnelRowReader rowBatch;
    protected AtomicBoolean eos = new AtomicBoolean(false);

    public StarRocksBeReadClient(String beNodeInfo, SourceConfig sourceConfig) {
        this.sourceConfig = sourceConfig;
        log.debug("Parse StarRocks BE address: '{}'.", beNodeInfo);
        String[] hostPort = beNodeInfo.split(":");
        if (hostPort.length != 2) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.CREATE_BE_READER_FAILED,
                    String.format("Format of StarRocks BE address[%s] is illegal", beNodeInfo));
        }
        this.ip = hostPort[0].trim();
        this.port = Integer.parseInt(hostPort[1].trim());
        TBinaryProtocol.Factory factory = new TBinaryProtocol.Factory();
        TSocket socket =
                new TSocket(
                        ip,
                        port,
                        sourceConfig.getConnectTimeoutMs(),
                        sourceConfig.getConnectTimeoutMs());
        try {
            socket.open();
        } catch (TTransportException e) {
            socket.close();
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.CREATE_BE_READER_FAILED,
                    "Failed to open socket",
                    e);
        }
        TProtocol protocol = factory.getProtocol(socket);
        client = new TStarrocksExternalService.Client(protocol);
    }

    public void openScanner(QueryPartition partition, SeaTunnelRowType seaTunnelRowType) {
        Set<Long> tabletIds = partition.getTabletIds();
        TScanOpenParams params = new TScanOpenParams();
        params.setTablet_ids(new ArrayList<>(tabletIds));
        params.setOpaqued_query_plan(partition.getQueryPlan());
        params.setCluster(DEFAULT_CLUSTER_NAME);
        params.setDatabase(sourceConfig.getDatabase());
        params.setTable(partition.getTable());
        params.setUser(sourceConfig.getUsername());
        params.setPasswd(sourceConfig.getPassword());
        params.setBatch_size(sourceConfig.getBatchRows());
        if (sourceConfig.getSourceOptionProps() != null) {
            params.setProperties(sourceConfig.getSourceOptionProps());
        }
        short keepAliveMin = (short) Math.min(Short.MAX_VALUE, sourceConfig.getKeepAliveMin());
        params.setKeep_alive_min(keepAliveMin);
        params.setQuery_timeout(sourceConfig.getQueryTimeoutSec());
        params.setMem_limit(sourceConfig.getMemLimit());
        log.info("open Scan params.mem_limit {} B", params.getMem_limit());
        log.info("open Scan params.keep-alive-min {} min", params.getKeep_alive_min());
        log.info("open Scan params.batch_size {}", params.getBatch_size());
        TScanOpenResult result = null;
        try {
            result = client.open_scanner(params);
            if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED,
                        "Failed to open scanner."
                                + result.getStatus().getStatus_code()
                                + result.getStatus().getError_msgs());
            }
        } catch (TException e) {
            throw new StarRocksConnectorException(
                    StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED, e.getMessage());
        }
        this.contextId = result.getContext_id();
        log.info(
                "Open scanner for {}:{} with context id {}, and there are {} tablets {}",
                ip,
                port,
                contextId,
                tabletIds.size(),
                tabletIds);
        this.eos.set(false);
        this.rowBatch = null;
        this.readerOffset = 0;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    public boolean hasNext() {
        boolean hasNext = false;
        // Arrow data was acquired synchronously during the iterative process
        if (!eos.get() && (rowBatch == null || !rowBatch.hasNext())) {
            if (rowBatch != null) {
                readerOffset += rowBatch.getReadRowCount();
                rowBatch.close();
            }
            TScanNextBatchParams nextBatchParams = new TScanNextBatchParams();
            nextBatchParams.setContext_id(contextId);
            nextBatchParams.setOffset(readerOffset);
            TScanBatchResult result;
            try {
                result = client.get_next(nextBatchParams);
                if (!TStatusCode.OK.equals(result.getStatus().getStatus_code())) {
                    throw new StarRocksConnectorException(
                            StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED,
                            "Failed to get next from be -> ip:["
                                    + ip
                                    + "] "
                                    + result.getStatus().getStatus_code()
                                    + " msg:"
                                    + result.getStatus().getError_msgs());
                }
                eos.set(result.isEos());
                if (!eos.get()) {

                    rowBatch =
                            new ArrowToSeatunnelRowReader(result.getRows(), seaTunnelRowType)
                                    .readArrow();
                }
            } catch (TException e) {
                throw new StarRocksConnectorException(
                        StarRocksConnectorErrorCode.SCAN_BE_DATA_FAILED, e.getMessage());
            }
        }
        hasNext = !eos.get();
        return hasNext;
    }

    public SeaTunnelRow getNext() {
        return rowBatch.next();
    }

    public void close() {
        log.info("Close reader for {}:{} with context id {}", ip, port, contextId);
        TScanCloseParams tScanCloseParams = new TScanCloseParams();
        tScanCloseParams.setContext_id(this.contextId);
        try {
            this.client.close_scanner(tScanCloseParams);
        } catch (TException e) {
            log.error("Failed to close reader {}:{} with context id {}", ip, port, contextId, e);
            throw new StarRocksConnectorException(CLOSE_BE_READER_FAILED, e);
        }
    }
}
