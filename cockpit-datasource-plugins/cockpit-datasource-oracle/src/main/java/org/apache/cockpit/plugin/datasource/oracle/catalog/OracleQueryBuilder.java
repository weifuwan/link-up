package org.apache.cockpit.plugin.datasource.oracle.catalog;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.enums.integration.WhereTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.api.utils.TimeUtil;
import org.apache.cockpit.plugin.datasource.oracle.param.OracleConnectionParam;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class OracleQueryBuilder {
    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{var:([^}]+)\\}");

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        OracleConnectionParam connectionParams = (OracleConnectionParam) JSONUtils.parseObject(connectionParam, OracleConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));

        if (StringUtils.isNotBlank(sourceParamObj.getString("table_path"))) {
            String tablePath = sourceParamObj.getString("table_path");
            if (StringUtils.isNotBlank(tablePath)) {
                String[] split = tablePath.split("\\.");
                if (split.length == 1) {
                    tablePath = connectionParams.getUsername().toUpperCase() + "." + split[0];
                }
            } else {
                throw new RuntimeException("table path is null");
            }
            jsonObject.put("table_path", tablePath);
            addWhereCondition(sourceParamObj, jsonObject);
        }

        if (StringUtils.isNotBlank(sourceParamObj.getString("query"))) {
            jsonObject.put("query", sourceParamObj.getString("query"));
        }

        jsonObject.put("plugin-type", DbType.ORACLE.getCode());
        jsonObject.put("dialect", DbType.ORACLE.getCode());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParams, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        OracleConnectionParam connectionParam = JSONUtils.parseObject(connectionParams, OracleConnectionParam.class);
        JSONObject jsonObject = new JSONObject();
        String sinkTablePath;
        if (sinkObj.getBoolean("autoCreateTable")) {
            sinkTablePath = sourceObj.getString("table_path");
            String[] split = sinkTablePath.split("\\.");
            if (split.length == 1) {
                sinkTablePath = connectionParam.getUsername() + "." + sinkTablePath;
            }
            jsonObject.put("generate_sink_sql", "true");
        } else {
            sinkTablePath = sinkObj.getString("table_path");
            String[] split = sinkTablePath.split("\\.");
            if (split.length == 1) {
                sinkTablePath = connectionParam.getUsername() + "." + sinkTablePath;
            }
        }

        jsonObject.put("jdbcUrl", connectionParam.getJdbcUrl());
        jsonObject.put("driver", connectionParam.getDriverClassName());
        jsonObject.put("username", connectionParam.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParam.getPassword()));
        jsonObject.put("table", sinkTablePath.toUpperCase());
        jsonObject.put("plugin-type", DbType.ORACLE.getCode());
        jsonObject.put("dialect", DbType.ORACLE.getCode());
        jsonObject.put("database", connectionParam.getDatabase());
        jsonObject.put("driver_location", connectionParam.getDriverLocation());
        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));
        return jsonObject;
    }

    private void addWhereCondition(JSONObject sourceParamObj, JSONObject resultJson) {
        WhereTypeEnum whereType = WhereTypeEnum.valueOf(sourceParamObj.getString("whereType").toUpperCase());
        if (whereType == WhereTypeEnum.DATA) {
            JSONArray filterConditions = sourceParamObj.getJSONArray("filterConditions");
            if (filterConditions != null && !filterConditions.isEmpty()) {
                String whereCondition = buildWhereCondition(filterConditions);
                resultJson.put("where_condition", whereCondition);
            }
        } else if (whereType == WhereTypeEnum.TIME) {
            JSONArray filterTimeConditions = sourceParamObj.getJSONArray("filterTimeConditions");
            if (filterTimeConditions != null && !filterTimeConditions.isEmpty()) {
                String whereCondition = buildTimeWhereCondition(filterTimeConditions);
                resultJson.put("where_condition", whereCondition);
            }
        } else {
            throw new RuntimeException("不支持where类型");
        }
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

    /**
     * 构建时间类型的WHERE条件，处理动态时间变量
     */
    private String buildTimeWhereCondition(JSONArray filterTimeConditions) {
        StringBuilder whereBuilder = new StringBuilder();

        for (int i = 0; i < filterTimeConditions.size(); i++) {
            JSONObject condition = filterTimeConditions.getJSONObject(i);

            String field = validateField(condition, "field", i);
            String operator = validateField(condition, "operator", i);
            String value = validateField(condition, "value", i);
            String logicalOperator = condition.getString("logicalOperator");

            if (i > 0 && StringUtils.isBlank(logicalOperator)) {
                throw new IllegalArgumentException(
                        String.format("filterTimeConditions[%d] 中的字段 'logicalOperator' 不能为空", i)
                );
            }

            String processedValue = replaceTimeVariables(value);
            String singleCondition = buildSingleTimeCondition(field, operator, processedValue);

            if (i == 0) {
                whereBuilder.append(singleCondition);
            } else {
                whereBuilder.append(" ").append(logicalOperator).append(" ").append(singleCondition);
            }
        }

        return "where " + whereBuilder.toString();
    }

    /**
     * 替换时间变量，将 ${var:xxx} 格式的变量替换为实际的时间值
     */
    private String replaceTimeVariables(String value) {
        Matcher matcher = VAR_PATTERN.matcher(value);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            String varExpression = matcher.group(1);
            String replacement = TimeUtil.resolveTimeVariable(varExpression);
            matcher.appendReplacement(result, replacement);
        }
        matcher.appendTail(result);

        return result.toString();
    }

    private String buildSingleTimeCondition(String field, String operator, String value) {
        return field + " " + operator + " TO_DATE('" + value.replace("'", "''") + "', 'YYYY-MM-DD HH24:MI:SS')";
    }

    private String buildSingleCondition(String field, String operator, String value) {
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
        if (sinkObj.getBoolean("autoCreateTable") != null && sinkObj.getBoolean("autoCreateTable")) {
            // get source table_path
            return sourceObj.getString("table_path");
        }
        return sinkObj.getString("table_path");
    }
}
