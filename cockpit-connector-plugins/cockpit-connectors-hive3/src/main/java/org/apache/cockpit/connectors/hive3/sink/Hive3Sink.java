package org.apache.cockpit.connectors.hive3.sink;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.connectors.api.catalog.Catalog;
import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
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
import org.apache.cockpit.connectors.api.sink.SaveModeHandler;
import org.apache.cockpit.connectors.api.sink.SeaTunnelSink;
import org.apache.cockpit.connectors.api.sink.SinkWriter;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.api.util.JdbcCatalogUtils;
import org.apache.cockpit.connectors.hive3.catalog.savemode.Hive3SaveModeHandler;
import org.apache.cockpit.connectors.hive3.config.FileFormat;
import org.apache.cockpit.connectors.hive3.config.FileSinkConfig;
import org.apache.cockpit.connectors.hive3.config.HadoopConf;
import org.apache.cockpit.connectors.hive3.config.HiveOptions;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorException;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategy;
import org.apache.cockpit.connectors.hive3.sink.writer.WriteStrategyFactory;
import org.apache.cockpit.connectors.hive3.storage.StorageFactory;
import org.apache.cockpit.connectors.hive3.util.HiveTableUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.cockpit.connectors.hive3.config.FileBaseSinkOptions.*;


@Slf4j
public class Hive3Sink
        implements SeaTunnelSink<SeaTunnelRow>, SupportSaveMode {

    private final CatalogTable catalogTable;
    private final ReadonlyConfig readonlyConfig;
    private final DataSaveMode dataSaveMode;
    private final JdbcDialect dialect;
    private final SchemaSaveMode schemaSaveMode;
    private TableSchema tableSchema;
    private transient WriteStrategy writeStrategy;
    private FileSinkConfig fileSinkConfig;
    private transient Table tableInformation;
    private final JdbcSinkConfig jdbcSinkConfig;
    private HadoopConf hadoopConf;

    public Hive3Sink(ReadonlyConfig readonlyConfig,
                     JdbcSinkConfig jdbcSinkConfig,
                     JdbcDialect dialect,
                     SchemaSaveMode schemaSaveMode,
                     DataSaveMode dataSaveMode,
                     CatalogTable catalogTable) {
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
        this.dialect = dialect;
    }

    @Override
    public void open() {
        this.hadoopConf = createHadoopConf(readonlyConfig);
        this.fileSinkConfig = generateFileSinkConfig(readonlyConfig, catalogTable);
        this.tableSchema = catalogTable.getTableSchema();
        this.tableInformation = getTableInformation();
    }

    private FileSinkConfig generateFileSinkConfig(
            ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        Table tableInformation = getTableInformation();
        Config pluginConfig = readonlyConfig.toConfig();
        List<String> sinkFields =
                tableInformation.getSd().getCols().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        List<String> partitionKeys =
                tableInformation.getPartitionKeys().stream()
                        .map(FieldSchema::getName)
                        .collect(Collectors.toList());
        sinkFields.addAll(partitionKeys);

        FileFormat fileFormat = HiveTableUtils.parseFileFormat(tableInformation);
        switch (fileFormat) {
            case TEXT:
                Map<String, String> parameters =
                        tableInformation.getSd().getSerdeInfo().getParameters();
                pluginConfig =
                        pluginConfig
                                .withValue(
                                        FILE_FORMAT_TYPE.key(),
                                        ConfigValueFactory.fromAnyRef(FileFormat.TEXT.toString()))
                                .withValue(
                                        FIELD_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("field.delim")))
                                .withValue(
                                        ROW_DELIMITER.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                parameters.get("line.delim")));
                break;
            case PARQUET:
                pluginConfig =
                        pluginConfig.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.PARQUET.toString()));
                break;
            case ORC:
                pluginConfig =
                        pluginConfig.withValue(
                                FILE_FORMAT_TYPE.key(),
                                ConfigValueFactory.fromAnyRef(FileFormat.ORC.toString()));
                break;
            default:
                throw new HiveConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Hive connector only support [text parquet orc] table now");
        }
        pluginConfig =
                pluginConfig
                        .withValue(
                                IS_PARTITION_FIELD_WRITE_IN_FILE.key(),
                                ConfigValueFactory.fromAnyRef(false))
                        .withValue(
                                FILE_NAME_EXPRESSION.key(),
                                ConfigValueFactory.fromAnyRef("${transactionId}"))
                        .withValue(
                                FILE_PATH.key(),
                                ConfigValueFactory.fromAnyRef(
                                        tableInformation.getSd().getLocation()))
                        .withValue(SINK_COLUMNS.key(), ConfigValueFactory.fromAnyRef(sinkFields))
                        .withValue(
                                PARTITION_BY.key(), ConfigValueFactory.fromAnyRef(partitionKeys));

        return new FileSinkConfig(pluginConfig, catalogTable.getSeaTunnelRowType());
    }

    @Override
    public String getPluginName() {
        return DbType.HIVE3.getCode();
    }


    @Override
    public SinkWriter<SeaTunnelRow> createWriter(SinkWriter.Context context) {
        TablePath sinkTablePath = catalogTable.getTablePath();
        return new Hive3SinkWriter(
                sinkTablePath,
                dialect,
                fileSinkConfig,
                tableSchema,
                getDatabaseTableSchema().orElse(null),
                getWriteStrategy(),
                hadoopConf,
                jdbcSinkConfig,
                readonlyConfig);

    }

    private Table getTableInformation() {
        if (tableInformation == null) {
            tableInformation = HiveTableUtils.getTableInfo(readonlyConfig);
        }
        return tableInformation;
    }

    private HadoopConf createHadoopConf(ReadonlyConfig readonlyConfig) {
        String hdfsLocation = getTableInformation().getSd().getLocation();

        /*
         * Build hadoop conf(support hdfs). The returned hadoop conf can be
         * CosConf、OssConf、S3Conf、HadoopConf so that HadoopFileSystemProxy can obtain the correct
         * Schema and FsHdfsImpl that can be filled into hadoop configuration in {@link
         * org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy#createConfiguration()}
         */
        HadoopConf hadoopConf =
                StorageFactory.getStorageType(hdfsLocation)
                        .buildHadoopConfWithReadOnlyConfig(readonlyConfig);
        readonlyConfig
                .getOptional(HiveOptions.HDFS_SITE_PATH)
                .ifPresent(hadoopConf::setHdfsSitePath);
        readonlyConfig.getOptional(HiveOptions.REMOTE_USER).ifPresent(hadoopConf::setRemoteUser);
        return hadoopConf;
    }


    private WriteStrategy getWriteStrategy() {
        if (writeStrategy == null) {
            writeStrategy = WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
            writeStrategy.setCatalogTable(catalogTable);
        }
        return writeStrategy;
    }

    private Optional<TableSchema> getDatabaseTableSchema() {
        Optional<Catalog> catalogOptional = getCatalog();
        FieldIdeEnum fieldIdeEnumEnum = readonlyConfig.get(JdbcOptions.FIELD_IDE);
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


    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.empty();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {

        if (catalogTable != null) {
            Optional<Catalog> catalogOptional = getCatalog();
            if (catalogOptional.isPresent()) {
                try {
                    Catalog catalog = catalogOptional.get();
                    FieldIdeEnum fieldIdeEnumEnum = readonlyConfig.get(JdbcOptions.FIELD_IDE);
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
                            new Hive3SaveModeHandler(
                                    schemaSaveMode,
                                    dataSaveMode,
                                    catalog,
                                    tablePath,
                                    catalogTable));
                } catch (Exception e) {
                    throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                }
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
}
