package org.apache.cockpit.connectors.dm.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.TableNotExistException;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcSinkConfig;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialect;
import org.apache.cockpit.connectors.api.jdbc.dialect.dialectenum.FieldIdeEnum;
import org.apache.cockpit.connectors.api.jdbc.exception.JdbcConnectorException;
import org.apache.cockpit.connectors.api.jdbc.sink.DataSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SchemaSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.SupportSaveMode;
import org.apache.cockpit.connectors.api.jdbc.sink.savemode.JdbcSaveModeHandler;
import org.apache.cockpit.connectors.api.sink.AbstractJdbcSinkWriter;
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.api.util.JdbcCatalogUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

import static org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;


@Slf4j
public class DmdbSink
        implements SeaTunnelSink<SeaTunnelRow>, SupportSaveMode {

    private final TableSchema tableSchema;

    private final JdbcSinkConfig jdbcSinkConfig;

    private final JdbcDialect dialect;

    private final ReadonlyConfig config;

    private final DataSaveMode dataSaveMode;

    private final SchemaSaveMode schemaSaveMode;

    private final CatalogTable catalogTable;

    public DmdbSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.catalogTable = catalogTable;
        this.tableSchema = catalogTable.getTableSchema();
    }

    @Override
    public String getPluginName() {
        return "DAMENG";
    }

    @Override
    public SinkWriter<SeaTunnelRow> createWriter(SinkWriter.Context context) {
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        TablePath sinkTablePath = catalogTable.getTablePath();
        AbstractJdbcSinkWriter sinkWriter;
        if (catalogTable.getTableSchema().getPrimaryKey() != null) {
            String keyName = tableSchema.getPrimaryKey().getColumnNames().get(0);
            int index = tableSchema.toPhysicalRowDataType().indexOf(keyName);
            if (index > -1) {
                return new DmdbSinkWriter(
                        sinkTablePath,
                        dialect,
                        jdbcSinkConfig,
                        tableSchema,
                        getDatabaseTableSchema().orElse(null),
                        index);
            }
        }
        sinkWriter =
                new DmdbSinkWriter(
                        sinkTablePath,
                        dialect,
                        jdbcSinkConfig,
                        tableSchema,
                        getDatabaseTableSchema().orElse(null),
                        null);
        return sinkWriter;
    }


    private Optional<TableSchema> getDatabaseTableSchema() {
        Optional<Catalog> catalogOptional = getCatalog();
        FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
        String fieldIde =
                fieldIdeEnumEnum == null
                        ? FieldIdeEnum.ORIGINAL.getValue()
                        : fieldIdeEnumEnum.getValue();
        TablePath tablePath =
                TablePath.of(
                        catalogTable.getTableId().getDatabaseName(),
                        catalogTable.getTableId().getSchemaName(),
                        CatalogUtils.quoteTableIdentifier(
                                catalogTable.getTableId().getTableName(), fieldIde));
        if (catalogOptional.isPresent()) {
            try (Catalog catalog = catalogOptional.get()) {
                catalog.open();
                return Optional.of(catalog.getTable(tablePath).getTableSchema());
            } catch (TableNotExistException e) {
                log.warn("table {} not exist when get the database catalog table", tablePath);
                return Optional.empty();
            }
        }
        return Optional.empty();
    }


    private Optional<Catalog> getCatalog() {
        if (StringUtils.isBlank(jdbcSinkConfig.getDatabase())) {
            return Optional.empty();
        }
        if (StringUtils.isBlank(jdbcSinkConfig.getTable())) {
            return Optional.empty();
        }
        // use query to write data can not support get catalog
        if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
            return Optional.empty();
        }
        return JdbcCatalogUtils.findCatalog(jdbcSinkConfig.getJdbcConnectionConfig(), dialect);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable != null) {
            Optional<Catalog> catalogOptional = getCatalog();
            if (catalogOptional.isPresent()) {
                try {
                    Catalog catalog = catalogOptional.get();
                    FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
                    String fieldIde =
                            fieldIdeEnumEnum == null
                                    ? FieldIdeEnum.ORIGINAL.getValue()
                                    : fieldIdeEnumEnum.getValue();
                    TablePath tablePath =
                            TablePath.of(
                                    catalogTable.getTableId().getDatabaseName(),
                                    catalogTable.getTableId().getSchemaName(),
                                    CatalogUtils.quoteTableIdentifier(
                                            catalogTable.getTableId().getTableName(), fieldIde));
                    catalogTable.getOptions().put("fieldIde", fieldIde);
                    return Optional.of(
                            new JdbcSaveModeHandler(
                                    schemaSaveMode,
                                    dataSaveMode,
                                    catalog,
                                    tablePath,
                                    catalogTable,
                                    config.get(JdbcOptions.CUSTOM_SQL),
                                    jdbcSinkConfig.isCreateIndex()));
                } catch (Exception e) {
                    throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
        return Optional.empty();
    }
}
