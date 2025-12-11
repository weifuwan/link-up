package org.apache.cockpit.plugin.datasource.elasticsearch.catalog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.elasticsearch.param.ElasticSearchConnectionParam;
import org.apache.commons.lang.StringUtils;

@Slf4j
public class ElasticSearchQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        ElasticSearchConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, ElasticSearchConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("index", sourceParamObj.getString("index"));
        jsonObject.put("plugin-type", DbType.ELASTICSEARCH.getCode());
        jsonObject.put("dialect", DbType.ELASTICSEARCH.getCode());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());

        addQueryCondition(sourceParamObj, jsonObject);

        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        ElasticSearchConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, ElasticSearchConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("jdbcUrl", connectionParams.getJdbcUrl());
        jsonObject.put("driver", connectionParams.getDriverClassName());
        jsonObject.put("username", connectionParams.getUsername());
        jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        jsonObject.put("index", getSinkIndex(sourceObj, sinkObj));
        jsonObject.put("plugin-type", DbType.ELASTICSEARCH.getCode());
        jsonObject.put("dialect", DbType.ELASTICSEARCH.getCode());
        jsonObject.put("driver_location", connectionParams.getDriverLocation());
        jsonObject.put("batch_size", sinkObj.getString("batch_size"));
        jsonObject.put("id_field", sinkObj.getString("id_field"));

        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    private void addQueryCondition(JSONObject sourceParamObj, JSONObject resultJson) {
        JSONArray filterConditions = sourceParamObj.getJSONArray("filterConditions");
        if (filterConditions != null && !filterConditions.isEmpty()) {
            JSONObject queryCondition = buildQueryCondition(filterConditions);
            resultJson.put("query", queryCondition);
        }
    }

    private JSONObject buildQueryCondition(JSONArray filterConditions) {
        JSONObject boolQuery = new JSONObject();
        JSONArray mustConditions = new JSONArray();

        for (int i = 0; i < filterConditions.size(); i++) {
            JSONObject condition = filterConditions.getJSONObject(i);

            String field = validateField(condition, "field", i);
            String operator = validateField(condition, "operator", i);
            String value = validateField(condition, "value", i);

            JSONObject termQuery = buildTermQuery(field, operator, value);
            mustConditions.add(termQuery);
        }

        boolQuery.put("must", mustConditions);
//        new JSONObject().put("bool", boolQuery)
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("bool", boolQuery);
        return jsonObject;

    }

    private JSONObject buildTermQuery(String field, String operator, String value) {
        JSONObject term = new JSONObject();

        switch (operator) {
            case "=":
                term.put("term", new JSONObject().put(field, value));
                break;
            case ">":
                term.put("range", new JSONObject().put(field, new JSONObject().put("gt", value)));
                break;
            case ">=":
                term.put("range", new JSONObject().put(field, new JSONObject().put("gte", value)));
                break;
            case "<":
                term.put("range", new JSONObject().put(field, new JSONObject().put("lt", value)));
                break;
            case "<=":
                term.put("range", new JSONObject().put(field, new JSONObject().put("lte", value)));
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }

        return term;
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

    private String getSinkIndex(JSONObject sourceObj, JSONObject sinkObj) {
        if (sinkObj.getBooleanValue("autoCreateIndex")) {
            return sourceObj.getString("index");
        }
        return sinkObj.getString("index");
    }
}
