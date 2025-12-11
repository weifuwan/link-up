package org.apache.cockpit.connectors.clickhouse.sink.file;

import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelException;
import org.apache.cockpit.connectors.clickhouse.config.FileReaderOption;
import org.apache.cockpit.connectors.clickhouse.shard.Shard;
import org.apache.cockpit.connectors.clickhouse.state.CKFileAggCommitInfo;
import org.apache.cockpit.connectors.clickhouse.state.CKFileCommitInfo;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ClickhouseFileSinkAggCommitter
         {

    private transient ClickhouseProxy proxy;
    private ClickhouseTable clickhouseTable;

    private final FileReaderOption fileReaderOption;

    public ClickhouseFileSinkAggCommitter(FileReaderOption readerOption) {
        fileReaderOption = readerOption;
    }

    public void init() {
        proxy =
                new ClickhouseProxy(
                        fileReaderOption.getShardMetadata().getDefaultShard().getNode());
        clickhouseTable =
                proxy.getClickhouseTable(
                        proxy.getClickhouseConnection(),
                        fileReaderOption.getShardMetadata().getDatabase(),
                        fileReaderOption.getShardMetadata().getTable());
    }

    public List<CKFileAggCommitInfo> commit(List<CKFileAggCommitInfo> aggregatedCommitInfo)
            throws IOException {
        aggregatedCommitInfo.forEach(
                commitInfo ->
                        commitInfo
                                .getDetachedFiles()
                                .forEach(
                                        (shard, files) -> {
                                            try {
                                                this.attachFileToClickhouse(shard, files);
                                            } catch (ClickHouseException e) {
                                                throw new SeaTunnelException(
                                                        "failed commit file to clickhouse", e);
                                            }
                                        }));
        return new ArrayList<>();
    }

    public CKFileAggCommitInfo combine(List<CKFileCommitInfo> commitInfos) {
        Map<Shard, List<String>> files = new HashMap<>();
        commitInfos.forEach(
                infos ->
                        infos.getDetachedFiles()
                                .forEach(
                                        (shard, file) -> {
                                            if (files.containsKey(shard)) {
                                                files.get(shard).addAll(file);
                                            } else {
                                                files.put(shard, file);
                                            }
                                        }));
        return new CKFileAggCommitInfo(files);
    }

    public void abort(List<CKFileAggCommitInfo> aggregatedCommitInfo) throws Exception {}

    private ClickhouseProxy getProxy() {
        if (proxy != null) {
            return proxy;
        }
        synchronized (this) {
            if (proxy != null) {
                return proxy;
            }
            proxy =
                    new ClickhouseProxy(
                            fileReaderOption.getShardMetadata().getDefaultShard().getNode());
            return proxy;
        }
    }

    public void close() throws IOException {
        if (proxy != null) {
            proxy.close();
        }
    }

    private void attachFileToClickhouse(Shard shard, List<String> clickhouseLocalFiles)
            throws ClickHouseException {
        ClickHouseRequest<?> request = getProxy().getClickhouseConnection(shard);
        for (String clickhouseLocalFile : clickhouseLocalFiles) {
            String attachSql =
                    String.format(
                            "ALTER TABLE %s ATTACH PART '%s'",
                            clickhouseTable.getLocalTableName(),
                            clickhouseLocalFile.substring(
                                    clickhouseLocalFile.lastIndexOf("/") + 1));

            log.info("Attach file to clickhouse table: {}", attachSql);
            ClickHouseResponse response = request.query(attachSql).executeAndWait();
            response.close();
        }
    }
}
