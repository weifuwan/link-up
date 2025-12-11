package org.apache.cockpit.connectors.elasticsearch.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.RowKind;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.RetryUtils;
import org.apache.cockpit.connectors.elasticsearch.client.EsRestClient;
import org.apache.cockpit.connectors.elasticsearch.dto.BulkResponse;
import org.apache.cockpit.connectors.elasticsearch.dto.IndexInfo;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.cockpit.connectors.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.cockpit.connectors.elasticsearch.serialize.ElasticsearchRowSerializer;
import org.apache.cockpit.connectors.elasticsearch.serialize.SeaTunnelRowSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * ElasticsearchSinkWriter is a sink writer that will write {@link SeaTunnelRow} to Elasticsearch.
 */
@Slf4j
public class ElasticsearchSinkWriter
        implements SinkWriter<SeaTunnelRow> {

    private final Context context;

    private final int maxBatchSize;

    private SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final List<String> requestEsList;
    private EsRestClient esRestClient;
    private RetryUtils.RetryMaterial retryMaterial;
    private static final long DEFAULT_SLEEP_TIME_MS = 200L;
    private final IndexInfo indexInfo;
    private TableSchema tableSchema;

    public ElasticsearchSinkWriter(
            Context context,
            CatalogTable catalogTable,
            ReadonlyConfig config,
            int maxBatchSize,
            int maxRetryCount) {
        this.context = context;
        this.maxBatchSize = maxBatchSize;

        this.indexInfo =
                new IndexInfo(catalogTable.getTableId().getTableName().toLowerCase(), config);
        esRestClient = EsRestClient.createInstance(config);
        this.seaTunnelRowSerializer =
                new ElasticsearchRowSerializer(
                        esRestClient.getClusterInfo(),
                        indexInfo,
                        catalogTable.getSeaTunnelRowType());

        this.requestEsList = new ArrayList<>(maxBatchSize);
        this.retryMaterial =
                new RetryUtils.RetryMaterial(maxRetryCount, true, exception -> true, DEFAULT_SLEEP_TIME_MS);
        this.tableSchema = catalogTable.getTableSchema();
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (RowKind.UPDATE_BEFORE.equals(element.getRowKind())) {
            return;
        }

        String indexRequestRow = seaTunnelRowSerializer.serializeRow(element);
        requestEsList.add(indexRequestRow);
        if (requestEsList.size() >= maxBatchSize) {
            bulkEsWithRetry(this.esRestClient, this.requestEsList);
        }
    }


    public synchronized void bulkEsWithRetry(
            EsRestClient esRestClient, List<String> requestEsList) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        if (requestEsList.size() > 0) {
                            String requestBody = String.join("\n", requestEsList) + "\n";
                            BulkResponse bulkResponse = esRestClient.bulk(requestBody);
                            if (bulkResponse.isErrors()) {
                                throw new ElasticsearchConnectorException(
                                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                                        "bulk es error: " + bulkResponse.getResponse());
                            }
                            return bulkResponse;
                        }
                        return null;
                    },
                    retryMaterial);
            requestEsList.clear();
        } catch (Exception e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "ElasticSearch execute batch statement error",
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        bulkEsWithRetry(this.esRestClient, this.requestEsList);
        esRestClient.close();
    }
}
