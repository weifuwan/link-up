package org.apache.cockpit.connectors.elasticsearch.exception;


import org.apache.cockpit.connectors.api.catalog.exception.SeaTunnelErrorCode;

public enum ElasticsearchConnectorErrorCode implements SeaTunnelErrorCode {
    BULK_RESPONSE_ERROR("ELASTICSEARCH-01", "Bulk es response error"),
    GET_ES_VERSION_FAILED("ELASTICSEARCH-02", "Get elasticsearch version failed"),
    SCROLL_REQUEST_ERROR("ELASTICSEARCH-03", "Fail to scroll request"),
    GET_INDEX_DOCS_COUNT_FAILED(
            "ELASTICSEARCH-04", "Get elasticsearch document index count failed"),
    LIST_INDEX_FAILED("ELASTICSEARCH-05", "List elasticsearch index failed"),
    DROP_INDEX_FAILED("ELASTICSEARCH-06", "Drop elasticsearch index failed"),
    CREATE_INDEX_FAILED("ELASTICSEARCH-07", "Create elasticsearch index failed"),
    ES_FIELD_TYPE_NOT_SUPPORT("ELASTICSEARCH-08", "Not support the elasticsearch field type"),
    CLEAR_INDEX_DATA_FAILED("ELASTICSEARCH-09", "Clear elasticsearch index data failed"),
    CHECK_INDEX_FAILED("ELASTICSEARCH-10", "Failed to check whether the index exists"),
    SOURCE_CONFIG_ERROR_01(
            "ELASTICSEARCH-11",
            "'index' or 'index_list' must be configured, with at least one being required."),
    SOURCE_CONFIG_ERROR_02("ELASTICSEARCH-12", "'query' must be configured."),
    ADD_FIELD_FAILED("ELASTICSEARCH-13", "Field add failed"),
    SCHEMA_CHANGE_FAILED("ELASTICSEARCH-14", "Schema change failed"),
    CREATE_PIT_FAILED("ELASTICSEARCH-15", "Create Point-in-Time failed"),
    DELETE_PIT_FAILED("ELASTICSEARCH-16", "Delete Point-in-Time failed"),
    SEARCH_WITH_PIT_FAILED("ELASTICSEARCH-17", "Search with Point-in-Time failed"),
    ;

    private final String code;
    private final String description;

    ElasticsearchConnectorErrorCode(String code, String description) {
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
