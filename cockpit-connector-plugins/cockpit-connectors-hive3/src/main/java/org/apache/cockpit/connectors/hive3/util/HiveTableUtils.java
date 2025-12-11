package org.apache.cockpit.connectors.hive3.util;

import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.catalog.exception.CommonErrorCodeDeprecated;
import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelRuntimeException;
import org.apache.cockpit.connectors.api.config.ReadonlyConfig;
import org.apache.cockpit.connectors.hive3.config.FileFormat;
import org.apache.cockpit.connectors.hive3.config.HiveConstants;
import org.apache.cockpit.connectors.hive3.config.HiveOptions;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorErrorCode;
import org.apache.cockpit.connectors.hive3.exception.HiveConnectorException;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveTableUtils {

    public static Table getTableInfo(ReadonlyConfig readonlyConfig) {
        String table = readonlyConfig.get(HiveOptions.TABLE_NAME);
        TablePath tablePath = TablePath.of(table);
        if (tablePath.getDatabaseName() == null || tablePath.getTableName() == null) {
            throw new SeaTunnelRuntimeException(
                    HiveConnectorErrorCode.HIVE_TABLE_NAME_ERROR, "Current table name is " + table);
        }
        try (HiveMetaStoreProxy hiveMetaStoreProxy = new HiveMetaStoreProxy(readonlyConfig)) {
            return hiveMetaStoreProxy.getTable(
                    tablePath.getDatabaseName(), tablePath.getTableName());
        }
    }


    public static FileFormat parseFileFormat(Table table) {
        String inputFormat = table.getSd().getInputFormat();
        if (HiveConstants.TEXT_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.TEXT;
        }
        if (HiveConstants.PARQUET_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.PARQUET;
        }
        if (HiveConstants.ORC_INPUT_FORMAT_CLASSNAME.equals(inputFormat)) {
            return FileFormat.ORC;
        }
        throw new HiveConnectorException(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                "Hive connector only support [text parquet orc] table now");
    }
}
