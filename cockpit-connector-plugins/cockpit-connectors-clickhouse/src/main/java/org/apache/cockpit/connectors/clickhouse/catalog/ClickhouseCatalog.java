package org.apache.cockpit.connectors.clickhouse.catalog;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseNode;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.catalog.exception.*;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseCatalogUtil;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseProxy;
import org.apache.cockpit.connectors.clickhouse.util.ClickhouseUtil;
import org.apache.cockpit.connectors.clickhouse.util.TypeConvertUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseBaseOptions.*;
import static org.apache.cockpit.connectors.clickhouse.config.ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE;


@Slf4j
public class ClickhouseCatalog implements Catalog {

    protected String defaultDatabase = "information_schema";
    private ReadonlyConfig readonlyConfig;
    private ClickhouseProxy proxy;
    private final String template;

    private String catalogName;

    public ClickhouseCatalog(ReadonlyConfig readonlyConfig, String catalogName) {
        this.readonlyConfig = readonlyConfig;
        this.catalogName = catalogName;
        this.template = readonlyConfig.get(SAVE_MODE_CREATE_TEMPLATE);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return proxy.listDatabases();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        return proxy.listTable(databaseName);
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        List<ClickHouseColumn> clickHouseColumns =
                proxy.getClickHouseColumns(tablePath.getFullNameWithQuoted());

        try {
            Optional<PrimaryKey> primaryKey =
                    proxy.getPrimaryKey(tablePath.getDatabaseName(), tablePath.getTableName());

            TableSchema.Builder builder = TableSchema.builder();
            primaryKey.ifPresent(builder::primaryKey);
            buildColumnsWithErrorCheck(
                    tablePath,
                    builder,
                    clickHouseColumns.iterator(),
                    column ->
                            PhysicalColumn.of(
                                    column.getColumnName(),
                                    TypeConvertUtil.convert(column),
                                    (long) column.getEstimatedLength(),
                                    column.getScale(),
                                    column.isNullable(),
                                    null,
                                    null));

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
            return CatalogTable.of(
                    tableIdentifier,
                    builder.build(),
                    buildConnectorOptions(tablePath),
                    Collections.emptyList(),
                    "");
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        log.debug("Create table :{}.{}", tablePath.getDatabaseName(), tablePath.getTableName());
        proxy.createTable(
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                template,
                table.getComment(),
                table.getTableSchema());
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        proxy.dropTable(tablePath, ignoreIfNotExists);
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            if (tableExists(tablePath)) {
                proxy.truncateTable(tablePath, ignoreIfNotExists);
            }
        } catch (Exception e) {
            throw new CatalogException("Truncate table failed", e);
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        try {
            proxy.executeSql(sql);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed EXECUTE SQL in catalog %s", sql), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        try {
            return proxy.isExistsData(tablePath.getFullName());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        proxy.createDatabase(tablePath.getDatabaseName(), ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        proxy.dropDatabase(tablePath.getDatabaseName(), ignoreIfNotExists);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "clickhouse");
        options.put("host", readonlyConfig.get(HOST));
        options.put("database", tablePath.getDatabaseName());
        return options;
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        List<ClickHouseNode> nodes = ClickhouseUtil.createNodes(readonlyConfig);
        Properties clickhouseProperties = new Properties();
        readonlyConfig
                .get(CLICKHOUSE_CONFIG)
                .forEach((key, value) -> clickhouseProperties.put(key, String.valueOf(value)));

        clickhouseProperties.put("user", readonlyConfig.get(USERNAME));
        clickhouseProperties.put("password", readonlyConfig.get(PASSWORD));
        proxy = new ClickhouseProxy(nodes.get(0));
    }

    @Override
    public void close() throws CatalogException {
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        checkArgument(StringUtils.isNotBlank(databaseName));
        return listDatabases().contains(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return proxy.tableExists(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            Preconditions.checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(
                    ClickhouseCatalogUtil.INSTANCE.getCreateTableSql(
                            template,
                            tablePath.getDatabaseName(),
                            tablePath.getTableName(),
                            catalogTable.get().getTableSchema(),
                            catalogTable.get().getComment(),
                            ClickhouseSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key()));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(
                    ClickhouseCatalogUtil.INSTANCE.getDropTableSql(tablePath, true));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(
                    ClickhouseCatalogUtil.INSTANCE.getTruncateTableSql(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(
                    ClickhouseCatalogUtil.INSTANCE.getCreateDatabaseSql(
                            tablePath.getDatabaseName(), true));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(
                    ClickhouseCatalogUtil.INSTANCE.getDropDatabaseSql(
                            tablePath.getDatabaseName(), true));
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
