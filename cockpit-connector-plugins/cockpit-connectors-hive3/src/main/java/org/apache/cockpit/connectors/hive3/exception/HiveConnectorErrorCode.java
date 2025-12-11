package org.apache.cockpit.connectors.hive3.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;

public enum HiveConnectorErrorCode implements SeaTunnelErrorCode {
    GET_HDFS_NAMENODE_HOST_FAILED("HIVE-01", "Get name node host from table location failed"),
    INITIALIZE_HIVE_METASTORE_CLIENT_FAILED("HIVE-02", "Initialize hive metastore client failed"),
    GET_HIVE_TABLE_INFORMATION_FAILED(
            "HIVE-03", "Get hive table information from hive metastore service failed"),
    HIVE_TABLE_NAME_ERROR("HIVE-04", "Hive table name is invalid"),
    LOAD_HIVE_BASE_HADOOP_CONFIG_FAILED("HIVE-05", "Load hive base hadoop config failed"),
    ;

    private final String code;
    private final String description;

    HiveConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
