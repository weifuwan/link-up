package org.apache.cockpit.connectors.doris.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.config.OptionRule;
import org.apache.cockpit.connectors.api.connector.TableSource;
import org.apache.cockpit.connectors.api.factory.Factory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactory;
import org.apache.cockpit.connectors.api.factory.TableSourceFactoryContext;
import org.apache.cockpit.connectors.api.source.SeaTunnelSource;
import org.apache.cockpit.connectors.api.source.SourceSplit;
import org.apache.cockpit.connectors.doris.catalog.DorisCatalog;
import org.apache.cockpit.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.cockpit.connectors.doris.config.DorisSourceConfig;
import org.apache.cockpit.connectors.doris.config.DorisSourceOptions;
import org.apache.cockpit.connectors.doris.config.DorisTableConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AutoService(Factory.class)
public class DorisSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return DorisSourceOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DorisSourceOptions.FENODES,
                        DorisSourceOptions.USERNAME,
                        DorisSourceOptions.PASSWORD)
                .optional(DorisSourceOptions.TABLE_LIST)
                .optional(DorisSourceOptions.DATABASE)
                .optional(DorisSourceOptions.TABLE)
                .optional(DorisSourceOptions.DORIS_FILTER_QUERY)
                .optional(DorisSourceOptions.DORIS_TABLET_SIZE)
                .optional(DorisSourceOptions.DORIS_REQUEST_CONNECT_TIMEOUT_MS)
                .optional(DorisSourceOptions.DORIS_REQUEST_READ_TIMEOUT_MS)
                .optional(DorisSourceOptions.DORIS_REQUEST_QUERY_TIMEOUT_S)
                .optional(DorisSourceOptions.DORIS_REQUEST_RETRIES)
                .optional(DorisSourceOptions.DORIS_DESERIALIZE_ARROW_ASYNC)
                .optional(DorisSourceOptions.DORIS_DESERIALIZE_QUEUE_SIZE)
                .optional(DorisSourceOptions.DORIS_READ_FIELD)
                .optional(DorisSourceOptions.QUERY_PORT)
                .optional(DorisSourceOptions.DORIS_BATCH_SIZE)
                .optional(DorisSourceOptions.DORIS_EXEC_MEM_LIMIT)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit>
    TableSource<T, SplitT> createSource(TableSourceFactoryContext context) {
        // Load the JDBC driver in to DriverManager
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception e) {
            log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
        }
        DorisSourceConfig dorisSourceConfig = DorisSourceConfig.of(context.getOptions());
        List<DorisTableConfig> dorisTableConfigList = dorisSourceConfig.getTableConfigList();
        Map<TablePath, DorisSourceTable> dorisSourceTables = new HashMap<>();

        DorisCatalogFactory dorisCatalogFactory = new DorisCatalogFactory();
        try (DorisCatalog catalog =
                (DorisCatalog) dorisCatalogFactory.createCatalog("doris", context.getOptions())) {
            catalog.open();
            for (DorisTableConfig dorisTableConfig : dorisTableConfigList) {
                CatalogTable table;
                TablePath tablePath = TablePath.of(dorisTableConfig.getTableIdentifier());
                String readFields = dorisTableConfig.getReadField();
                try {
                    List<String> readFiledList = null;
                    if (StringUtils.isNotBlank(readFields)) {
                        readFiledList =
                                Arrays.stream(readFields.split(","))
                                        .map(String::trim)
                                        .collect(Collectors.toList());
                    }

                    table = catalog.getTable(tablePath, readFiledList);
                } catch (Exception e) {
                    log.error("create source error");
                    throw e;
                }
                dorisSourceTables.put(
                        tablePath,
                        DorisSourceTable.builder()
                                .catalogTable(table)
                                .tablePath(tablePath)
                                .readField(readFields)
                                .filterQuery(dorisTableConfig.getFilterQuery())
                                .batchSize(dorisTableConfig.getBatchSize())
                                .tabletSize(dorisTableConfig.getTabletSize())
                                .execMemLimit(dorisTableConfig.getExecMemLimit())
                                .build());
            }
        }
        return () ->
                (SeaTunnelSource<T, SplitT>)
                        new DorisSource(dorisSourceConfig, dorisSourceTables);
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return DorisSource.class;
    }
}
