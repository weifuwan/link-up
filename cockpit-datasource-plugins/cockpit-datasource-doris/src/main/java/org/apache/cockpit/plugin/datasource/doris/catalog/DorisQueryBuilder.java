package org.apache.cockpit.plugin.datasource.doris.catalog;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.enums.integration.WhereTypeEnum;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.api.utils.TimeUtil;
import org.apache.cockpit.plugin.datasource.doris.param.DorisConnectionParam;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class DorisQueryBuilder {

    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{var:([^}]+)\\}");

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        DorisConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, DorisConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("fenodes", connectionParams.getHost() + ":" + 8030);
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table", sourceParamObj.getString("table_path"));
        jsonObject.put("plugin-type", DbType.DORIS.getCode());
        jsonObject.put("dialect", DbType.DORIS.getCode());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());

        if (StringUtils.isNotBlank(sourceParamObj.getString("table_path"))) {
            jsonObject.put("table_path", buildTablePath(connectionParams, sourceParamObj));
            addWhereCondition(sourceParamObj, jsonObject);
        }
        if (StringUtils.isNotBlank(sourceParamObj.getString("query"))) {
            jsonObject.put("query", sourceParamObj.getString("query"));
        }


        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        JSONObject jsonObject = new JSONObject();
        String sinkTablePath;
        if (sinkObj.getBoolean("autoCreateTable")) {
            sinkTablePath = sourceObj.getString("table_path");
            if (StringUtils.isNotBlank(sinkTablePath)) {
                String[] split = sinkTablePath.split("\\.");
                if (split.length == 2) {
                    sinkTablePath = split[1];
                }
            }
            jsonObject.put("generate_sink_sql", "true");
        } else {
            sinkTablePath = sinkObj.getString("table_path");
            if (StringUtils.isNotBlank(sinkTablePath)) {
                String[] split = sinkTablePath.split("\\.");
                if (split.length == 2) {
                    sinkTablePath = split[1];
                }
            }
        }

        DorisConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, DorisConnectionParam.class);

        jsonObject.put("fenodes", connectionParams.getHost() + ":" + 8030);
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table", sinkTablePath);
        jsonObject.put("plugin-type", DbType.DORIS.getCode());
        jsonObject.put("dialect", DbType.DORIS.getCode());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));

        JSONObject dorisConfig = buildDorisConfig(sinkObj);
        jsonObject.put("doris.config", dorisConfig.toJSONString());

        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    private JSONObject buildDorisConfig(JSONObject sinkObj) {
        JSONObject dorisConfig = new JSONObject();

        dorisConfig.put("format", "json");
        dorisConfig.put("read_json_by_line", "true");

        if (sinkObj.containsKey("data_format")) {
            dorisConfig.put("format", sinkObj.getString("data_format"));
        }
        if (sinkObj.containsKey("read_json_by_line")) {
            dorisConfig.put("read_json_by_line", sinkObj.getString("read_json_by_line"));
        }

        return dorisConfig;
    }

    private String buildTablePath(DorisConnectionParam connectionParams, JSONObject sourceParamObj) {
        return String.format("%s.%s", connectionParams.getDatabase(), sourceParamObj.getString("table_path"));
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

            // 处理时间变量替换
            String processedValue = replaceTimeVariables(value);
            String singleCondition = buildSingleCondition(field, operator, processedValue);

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

}
