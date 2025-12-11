package org.apache.cockpit.connectors.elasticsearch.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.source.JdbcSourceTable;
import org.apache.cockpit.connectors.api.source.Collector;
import org.apache.cockpit.connectors.api.source.SourceReader;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SeaTunnelRowType;
import org.apache.cockpit.connectors.elasticsearch.client.EsRestClient;
import org.apache.cockpit.connectors.elasticsearch.config.ElasticsearchConfig;
import org.apache.cockpit.connectors.elasticsearch.config.SearchApiTypeEnum;
import org.apache.cockpit.connectors.elasticsearch.config.SearchTypeEnum;
import org.apache.cockpit.connectors.elasticsearch.dto.source.PointInTimeResult;
import org.apache.cockpit.connectors.elasticsearch.dto.source.ScrollResult;
import org.apache.cockpit.connectors.elasticsearch.serialize.source.DefaultSeaTunnelRowDeserializer;
import org.apache.cockpit.connectors.elasticsearch.serialize.source.ElasticsearchRecord;
import org.apache.cockpit.connectors.elasticsearch.serialize.source.SeaTunnelRowDeserializer;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ElasticsearchSourceReader
        implements SourceReader<SeaTunnelRow, ElasticsearchSourceSplit> {

    SourceReader.Context context;

    private final ReadonlyConfig connConfig;

    private EsRestClient esRestClient;

    Deque<ElasticsearchSourceSplit> splits = new LinkedList<>();
    boolean noMoreSplit;

    private final long pollNextWaitTime = 1000L;

    public ElasticsearchSourceReader(SourceReader.Context context, ReadonlyConfig connConfig) {
        this.context = context;
        this.connConfig = connConfig;
    }

    @Override
    public void open() {
        esRestClient = EsRestClient.createInstance(this.connConfig);
    }

    @Override
    public CatalogTable getJdbcSourceTables() {
        return null;
    }

    @Override
    public void close() throws IOException {
        esRestClient.close();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            ElasticsearchSourceSplit split = splits.poll();
            if (split != null) {
                SeaTunnelRowType seaTunnelRowType = split.getSeaTunnelRowType();
                ElasticsearchConfig sourceIndexInfo = split.getElasticsearchConfig();
                scrollSearchResult(seaTunnelRowType, sourceIndexInfo, output);
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded ELasticsearch source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(pollNextWaitTime);
            }
        }
    }

    private void scrollSearchResult(
            SeaTunnelRowType seaTunnelRowType,
            ElasticsearchConfig sourceIndexInfo,
            Collector<SeaTunnelRow> output) throws Exception {

        SeaTunnelRowDeserializer deserializer =
                new DefaultSeaTunnelRowDeserializer(seaTunnelRowType);

        // SQL client
        if (SearchTypeEnum.SQL.equals(sourceIndexInfo.getSearchType())) {
            log.info("Using SQL query for index: {}", sourceIndexInfo.getIndex());
            ScrollResult scrollResult =
                    esRestClient.searchBySql(
                            sourceIndexInfo.getSqlQuery(), sourceIndexInfo.getScrollSize());

            outputFromScrollResult(scrollResult, sourceIndexInfo, output, deserializer);
            while (StringUtils.isNotEmpty(scrollResult.getScrollId())) {
                scrollResult =
                        esRestClient.searchWithSql(
                                scrollResult.getScrollId(), scrollResult.getColumnNodes());
                outputFromScrollResult(scrollResult, sourceIndexInfo, output, deserializer);
            }
        } else {
            // Check if we should use PIT API
            if (SearchApiTypeEnum.PIT.equals(sourceIndexInfo.getSearchApiType())) {
                log.info("Using Point-in-Time (PIT) API for index: {}", sourceIndexInfo.getIndex());
                searchWithPointInTime(sourceIndexInfo, output, deserializer);
            } else {
                log.info("Using Scroll API for index: {}", sourceIndexInfo.getIndex());
                ScrollResult scrollResult =
                        esRestClient.searchByScroll(
                                sourceIndexInfo.getIndex(),
                                sourceIndexInfo.getSource(),
                                sourceIndexInfo.getQuery(),
                                sourceIndexInfo.getScrollTime(),
                                sourceIndexInfo.getScrollSize());
                outputFromScrollResult(scrollResult, sourceIndexInfo, output, deserializer);
                while (scrollResult.getDocs() != null && !scrollResult.getDocs().isEmpty()) {
                    scrollResult =
                            esRestClient.searchWithScrollId(
                                    scrollResult.getScrollId(), sourceIndexInfo.getScrollTime());
                    outputFromScrollResult(scrollResult, sourceIndexInfo, output, deserializer);
                }
            }
        }
    }

    /**
     * Search using Point-in-Time API.
     *
     * @param sourceIndexInfo The Elasticsearch configuration
     * @param output The collector to output rows
     * @param deserializer The deserializer to convert Elasticsearch records to SeaTunnel rows
     */
    private void searchWithPointInTime(
            ElasticsearchConfig sourceIndexInfo,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) throws Exception {

        // Create a PIT
        String pitId =
                esRestClient.createPointInTime(
                        sourceIndexInfo.getIndex(), sourceIndexInfo.getPitKeepAlive());
        sourceIndexInfo.setPitId(pitId);
        log.info(
                "Created Point-in-Time with ID: {} for index: {}",
                pitId,
                sourceIndexInfo.getIndex());

        try {
            // Initial search
            PointInTimeResult pitResult =
                    esRestClient.searchWithPointInTime(
                            pitId,
                            sourceIndexInfo.getSource(),
                            sourceIndexInfo.getQuery(),
                            sourceIndexInfo.getPitBatchSize(),
                            null, // No search_after for first request
                            sourceIndexInfo.getPitKeepAlive());

            // Output the results
            outputFromPitResult(pitResult, sourceIndexInfo, output, deserializer);

            // Continue searching while there are more results
            while (pitResult.isHasMore()) {
                // Update the PIT ID and search_after values for the next request
                sourceIndexInfo.setPitId(pitResult.getPitId());
                sourceIndexInfo.setSearchAfter(pitResult.getSearchAfter());

                // Execute the next search
                pitResult =
                        esRestClient.searchWithPointInTime(
                                sourceIndexInfo.getPitId(),
                                sourceIndexInfo.getSource(),
                                sourceIndexInfo.getQuery(),
                                sourceIndexInfo.getPitBatchSize(),
                                sourceIndexInfo.getSearchAfter(),
                                sourceIndexInfo.getPitKeepAlive());

                // Output the results
                outputFromPitResult(pitResult, sourceIndexInfo, output, deserializer);
            }
        } finally {
            // Always clean up the PIT when done
            if (pitId != null) {
                try {
                    esRestClient.deletePointInTime(pitId);
                } catch (Exception e) {
                    log.warn("Failed to delete Point-in-Time with ID: " + pitId, e);
                }
            }
        }
    }

    private void outputFromScrollResult(
            ScrollResult scrollResult,
            ElasticsearchConfig elasticsearchConfig,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) throws Exception {
        List<String> source = elasticsearchConfig.getSource();
        String tableId = elasticsearchConfig.getCatalogTable().getTablePath().toString();
        for (Map<String, Object> doc : scrollResult.getDocs()) {
            SeaTunnelRow seaTunnelRow =
                    deserializer.deserialize(new ElasticsearchRecord(doc, source, tableId));
            output.collect(seaTunnelRow);
        }
    }

    /**
     * Output rows from a Point-in-Time search result.
     *
     * @param pitResult The Point-in-Time search result
     * @param elasticsearchConfig The Elasticsearch configuration
     * @param output The collector to output rows
     * @param deserializer The deserializer to convert Elasticsearch records to SeaTunnel rows
     */
    private void outputFromPitResult(
            PointInTimeResult pitResult,
            ElasticsearchConfig elasticsearchConfig,
            Collector<SeaTunnelRow> output,
            SeaTunnelRowDeserializer deserializer) throws Exception {
        List<String> source = elasticsearchConfig.getSource();
        String tableId = elasticsearchConfig.getCatalogTable().getTablePath().toString();
        for (Map<String, Object> doc : pitResult.getDocs()) {
            SeaTunnelRow seaTunnelRow =
                    deserializer.deserialize(new ElasticsearchRecord(doc, source, tableId));
            output.collect(seaTunnelRow);
        }
    }

}
