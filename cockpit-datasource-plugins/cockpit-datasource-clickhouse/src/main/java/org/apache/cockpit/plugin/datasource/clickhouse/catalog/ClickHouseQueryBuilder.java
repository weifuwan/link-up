package org.apache.cockpit.plugin.datasource.clickhouse.catalog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.clickhouse.param.ClickHouseConnectionParam;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ClickHouseQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        ClickHouseConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, ClickHouseConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table_path", buildTablePath(connectionParams, sourceParamObj));
        jsonObject.put("plugin-type", DbType.CLICKHOUSE.getCode());
        jsonObject.put("dialect", DbType.CLICKHOUSE.getCode());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());

        addWhereCondition(sourceParamObj, jsonObject);
        addClickHouseSpecificParams(sourceParamObj, jsonObject);

        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        ClickHouseConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, ClickHouseConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("host", connectionParams.getHost() + ":" + connectionParams.getPort());

        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table", getSinkTablePath(sourceObj, sinkObj));
        jsonObject.put("plugin-type", DbType.CLICKHOUSE.getCode());
        jsonObject.put("dialect", DbType.CLICKHOUSE.getCode());
        if (StringUtils.isNotBlank(connectionParams.getDatabase())) {
            jsonObject.put("generate_sink_sql", "true");
            jsonObject.put("database", connectionParams.getDatabase());
        }
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));


        // ClickHouse特定的写入参数
        addClickHouseSinkParams(sinkObj, jsonObject);

        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    private String buildTablePath(ClickHouseConnectionParam connectionParams, JSONObject sourceParamObj) {
        return String.format("%s.%s", connectionParams.getDatabase(), sourceParamObj.getString("table_path"));
    }

    private void addWhereCondition(JSONObject sourceParamObj, JSONObject resultJson) {
        JSONArray filterConditions = sourceParamObj.getJSONArray("filterConditions");
        if (filterConditions != null && !filterConditions.isEmpty()) {
            String whereCondition = buildWhereCondition(filterConditions);
            resultJson.put("where_condition", whereCondition);
        }
    }

    private void addClickHouseSpecificParams(JSONObject sourceParamObj, JSONObject resultJson) {
        // ClickHouse特定的查询参数
        if (sourceParamObj.containsKey("max_execution_time")) {
            resultJson.put("max_execution_time", sourceParamObj.getString("max_execution_time"));
        }
        if (sourceParamObj.containsKey("max_block_size")) {
            resultJson.put("max_block_size", sourceParamObj.getString("max_block_size"));
        }
        if (sourceParamObj.containsKey("distributed_group_by_no_merge")) {
            resultJson.put("distributed_group_by_no_merge", sourceParamObj.getBoolean("distributed_group_by_no_merge"));
        }
    }

    private void addClickHouseSinkParams(JSONObject sinkObj, JSONObject jsonObject) {
        // 基础配置
        Map<String, String> baseConfig = new HashMap<>();
        baseConfig.put("compress", "0");

        // 创建配置对象
        JSONObject clickhouseConfig = new JSONObject();
        clickhouseConfig.putAll(baseConfig);

        jsonObject.put("clickhouse.config", clickhouseConfig);
    }


    private String buildWhereCondition(JSONArray filterConditions) {
        StringBuilder whereBuilder = new StringBuilder();

        for (int i = 0; i < filterConditions.size(); i++) {
            JSONObject condition = filterConditions.getJSONObject(i);

            String field = validateField(condition, "field", i);
            String operator = validateField(condition, "operator", i);
            String value = validateField(condition, "value", i);
            String logicalOperator = condition.getString("logicalOperator");

            if (i > 0 && StringUtils.isBlank(logicalOperator)) {
                throw new IllegalArgumentException(
                        String.format("filterConditions[%d] 中的字段 'logicalOperator' 不能为空", i)
                );
            }

            String singleCondition = buildSingleCondition(field, operator, value);
            if (i == 0) {
                whereBuilder.append(singleCondition);
            } else {
                whereBuilder.append(" ").append(logicalOperator).append(" ").append(singleCondition);
            }
        }

        return "where " + whereBuilder.toString();
    }

    private String buildSingleCondition(String field, String operator, String value) {
        // ClickHouse特殊处理，比如数组操作符等
        if ("IN".equalsIgnoreCase(operator)) {
            return field + " " + operator + " (" + value + ")";
        }
        return field + " " + operator + " '" + value.replace("'", "''") + "'";
    }

    private String validateField(JSONObject condition, String fieldName, int index) {
        String value = condition.getString(fieldName);
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(
                    String.format("filterConditions[%d] 中的字段 '%s' 不能为空", index, fieldName)
            );
        }
        return value.trim();
    }

    private String getSinkTablePath(JSONObject sourceObj, JSONObject sinkObj) {
        if (sinkObj.getBoolean("autoCreateTable")) {
            return sourceObj.getString("table_path");
        }
        return sinkObj.getString("table_path");
    }
}