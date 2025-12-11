package org.apache.cockpit.plugin.datasource.hive.catalog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.hive.param.HiveConnectionParam;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class HiveQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        HiveConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, HiveConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("table_path", buildTablePath(connectionParams, sourceParamObj));
        jsonObject.put("plugin-type", DbType.HIVE3.getCode());
        jsonObject.put("dialect", DbType.HIVE3.getCode());

        // 添加 Kerberos 认证信息
        if (StringUtils.isNotBlank(connectionParams.getLoginUserKeytabUsername())) {
            jsonObject.put("principal", connectionParams.getLoginUserKeytabUsername());
        }
        if (StringUtils.isNotBlank(connectionParams.getLoginUserKeytabPath())) {
            jsonObject.put("keytab", connectionParams.getLoginUserKeytabPath());
        }
        if (StringUtils.isNotBlank(connectionParams.getJavaSecurityKrb5Conf())) {
            jsonObject.put("krb5_conf", connectionParams.getJavaSecurityKrb5Conf());
        }

        addWhereCondition(sourceParamObj, jsonObject);

        log.info("build hive source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        HiveConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, HiveConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        if (StringUtils.isNotBlank(connectionParams.getDatabase())) {
            jsonObject.put("generate_sink_sql", "true");
            jsonObject.put("database", connectionParams.getDatabase());
        }
        jsonObject.put("table_name", connectionParams.getDatabase() + "." + getSinkTablePath(sourceObj, sinkObj));
        jsonObject.put("plugin-type", DbType.HIVE3.getCode());
        jsonObject.put("dialect", DbType.HIVE3.getCode());
        jsonObject.put("metastore_uri", "thrift://" + connectionParams.getHost() + ":9083");
        jsonObject.put("thriftUrl", "thrift://" + connectionParams.getHost() + ":9083");
        jsonObject.put("file_format_type", sinkObj.getString("file_format_type"));
        jsonObject.put("partition_field", sinkObj.getString("partition_field"));

        // 添加 Kerberos 认证信息
        if (StringUtils.isNotBlank(connectionParams.getLoginUserKeytabUsername())) {
            jsonObject.put("principal", connectionParams.getLoginUserKeytabUsername());
        }
        if (StringUtils.isNotBlank(connectionParams.getLoginUserKeytabPath())) {
            jsonObject.put("keytab", connectionParams.getLoginUserKeytabPath());
        }

        jsonObject.put("enable_upsert", sinkObj.getBoolean("enable_upsert"));
        jsonObject.put("data_save_mode", sinkObj.getString("data_save_mode"));
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));

        log.info("build hive sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }


    private String buildTablePath(HiveConnectionParam connectionParams, JSONObject sourceParamObj) {
        return String.format("%s.%s", connectionParams.getDatabase(), sourceParamObj.getString("table_path"));
    }

    private void addWhereCondition(JSONObject sourceParamObj, JSONObject resultJson) {
        JSONArray filterConditions = sourceParamObj.getJSONArray("filterConditions");
        if (filterConditions != null && !filterConditions.isEmpty()) {
            String whereCondition = buildWhereCondition(filterConditions);
            resultJson.put("where_condition", whereCondition);
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
        if (sinkObj.getBoolean("autoCreateTable")) {
            return sourceObj.getString("table_path");
        }
        return sinkObj.getString("table_path");
    }
}
