package org.apache.cockpit.plugin.datasource.mongodb.catalog;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.enums.DbType;
import org.apache.cockpit.common.utils.JSONUtils;
import org.apache.cockpit.plugin.datasource.api.utils.PasswordUtils;
import org.apache.cockpit.plugin.datasource.mongodb.param.MongoConnectionParam;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

@Slf4j
public class MongoQueryBuilder {

    public JSONObject buildSourceJson(String connectionParam, String sourceParam) {
        JSONObject sourceParamObj = JSONObject.parseObject(sourceParam);
        MongoConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, MongoConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("uri", connectionParams.getConnectionString());
        jsonObject.put("database", connectionParams.getDatabase());
        jsonObject.put("collection", sourceParamObj.getString("table_path"));

        if (connectionParams.getUsername() != null) {
            jsonObject.put("username", connectionParams.getUsername());
        }
        if (connectionParams.getPassword() != null) {
            jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        }

        jsonObject.put("plugin-type", DbType.MONGODB.getCode());
        jsonObject.put("dialect", DbType.MONGODB.getCode());

        addFilterCondition(sourceParamObj, jsonObject);
        addProjection(sourceParamObj, jsonObject);

        try {
            JSONObject schema = inferSchema(connectionParams.getConnectionString(), connectionParams.getDatabase(), sourceParamObj.getString("table_path"));
            if (schema != null && !schema.isEmpty()) {
                jsonObject.put("schema", schema);
            } else {
                log.warn("Failed to infer schema for collection: {}.{}", connectionParams.getDatabase(), sourceParamObj.getString("table_path"));
            }
        } catch (Exception e) {
            log.error("An error occurred while inferring schema for collection: {}.{}", connectionParams.getDatabase(), sourceParamObj.getString("table_path"), e);
            throw new RuntimeException("Schema inference failed", e);
        }

        log.info("build source json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    /**
     * 根据 MongoDB 连接信息和集合名，推断其 Schema。
     *
     * @param uri            MongoDB 连接字符串
     * @param databaseName   数据库名
     * @param collectionName 集合名
     * @return 包含 "fields" 的 schema JSON 对象
     */
    private JSONObject inferSchema(String uri, String databaseName, String collectionName) {
        // 1. 创建 MongoClient
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // 2. 检查集合是否为空
            long count = collection.countDocuments();
            if (count == 0) {
                log.warn("Collection {} is empty, cannot infer schema.", collectionName);
                return null;
            }

            // 3. 抽样文档。使用聚合管道进行随机抽样，效率更高。
            List<Document> sampleDocuments = new ArrayList<>();
            int actualSampleSize = (int) Math.min(100, count);

            Bson sampleStage = Aggregates.sample(actualSampleSize);
            collection.aggregate(Collections.singletonList(sampleStage)).into(sampleDocuments);

            // 4. 推断字段类型
            Map<String, Object> fields = new HashMap<>();
            for (Document doc : sampleDocuments) {
                inferDocumentFields(doc, fields);
            }

            // 5. 构建并返回 schema JSON
            JSONObject schemaJson = new JSONObject();
            schemaJson.put("fields", fields);
            return schemaJson;
        }
    }

    /**
     * 递归地推断单个 Document 内所有字段的类型，并合并到一个全局字段类型 Map 中。
     *
     * @param doc    要分析的 BSON Document
     * @param fields 用于存储全局字段类型的 Map
     */
    private void inferDocumentFields(Document doc, Map<String, Object> fields) {
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            String fieldType = getFieldTypeString(fieldValue);

            // 如果字段已存在，检查类型是否兼容。这里简化处理，只保留首次遇到的类型。
            // 更复杂的逻辑可以是：如果遇到不同类型，可以标记为 "mixed" 或取更宽泛的类型。
            if (!fields.containsKey(fieldName)) {
                fields.put(fieldName, fieldType);
            } else {
                // (可选) 检查类型兼容性
                String existingType = fields.get(fieldName).toString();
                if (!existingType.equals(fieldType)) {
                    log.warn("Field '{}' has mixed types. Found '{}' and '{}'. Using '{}'.",
                            fieldName, existingType, fieldType, existingType);
                }
            }

            // 如果字段值是内嵌文档或数组，需要递归处理
            if (fieldValue instanceof Document) {
                // 对于内嵌文档，我们将其类型表示为一个嵌套的 Map
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedFields = (Map<String, Object>) fields.computeIfAbsent(fieldName, k -> new HashMap<>());
                inferDocumentFields((Document) fieldValue, nestedFields);
            } else if (fieldValue instanceof List<?>) {
                // 对于数组，我们只推断其第一个非空元素的类型
                List<?> list = (List<?>) fieldValue;
                if (!list.isEmpty()) {
                    Object firstElement = list.get(0);
                    String elementType = getFieldTypeString(firstElement);
                    fields.put(fieldName, "array<" + elementType + ">");

                    // 如果数组元素是文档，也需要递归处理其结构
                    if (firstElement instanceof Document) {
                        Map<String, Object> dummyParent = new HashMap<>();
                        inferDocumentFields((Document) firstElement, dummyParent);
                    }
                }
            }
        }
    }

    /**
     * 将 BSON 数据类型转换为你所期望的字符串表示。
     *
     * @param value BSON 字段值
     * @return 类型的字符串表示，如 "string", "int", "map<string, string>"
     */
    private String getFieldTypeString(Object value) {
        if (value == null) {
            return "string";
        } else if (value instanceof String) {
            return "string";
        } else if (value instanceof Integer) {
            return "int";
        } else if (value instanceof Long) {
            return "bigint";
        } else if (value instanceof Double) {
            return "double";
        } else if (value instanceof Boolean) {
            return "boolean";
        } else if (value instanceof java.util.Date) {
            return "date";
        } else if (value instanceof org.bson.types.ObjectId) {
            return "string";
        } else if (value instanceof org.bson.types.BSONTimestamp) {
            return "timestamp";
        } else if (value instanceof org.bson.types.Decimal128) {
            return "decimal(38, 18)";
        } else if (value instanceof byte[]) {
            return "bytes";
        } else if (value instanceof Document) {
            return "map";
        } else if (value instanceof List<?>) {
            return "array";
        } else {
            log.info("Unmapped BSON type: {}", value.getClass().getName());
            return value.getClass().getSimpleName().toLowerCase();
        }
    }

    public JSONObject buildSinkJson(String connectionParam, String globalParam) {
        JSONObject globalParamObj = JSONObject.parseObject(globalParam);
        JSONObject sourceObj = globalParamObj.getJSONObject("source");
        JSONObject sinkObj = globalParamObj.getJSONObject("sink");

        MongoConnectionParam connectionParams = JSONUtils.parseObject(connectionParam, MongoConnectionParam.class);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("connectionString", connectionParams.getConnectionString());
        if (StringUtils.isNotBlank(connectionParams.getDatabase())) {
            jsonObject.put("generate_sink_sql", "true");
            jsonObject.put("database", connectionParams.getDatabase());
        }
        jsonObject.put("collection", getSinkCollection(sourceObj, sinkObj));

        if (connectionParams.getUsername() != null) {
            jsonObject.put("username", connectionParams.getUsername());
        }
        if (connectionParams.getPassword() != null) {
            jsonObject.put("password", PasswordUtils.decodePassword(connectionParams.getPassword()));
        }

        jsonObject.put("uri", connectionParams.getConnectionString());

        jsonObject.put("plugin-type", DbType.MONGODB.getCode());
        jsonObject.put("dialect", DbType.MONGODB.getCode());
        jsonObject.put("write_mode", sinkObj.getString("write_mode"));
        jsonObject.put("batch_size", sinkObj.getInteger("batch_size"));
        jsonObject.put("ordered", sinkObj.getBoolean("ordered"));

        log.info("build sink json is " + jsonObject.toJSONString());
        return jsonObject;
    }

    private void addFilterCondition(JSONObject sourceParamObj, JSONObject resultJson) {
        JSONArray filterConditions = sourceParamObj.getJSONArray("filterConditions");
        if (filterConditions != null && !filterConditions.isEmpty()) {
            JSONObject filter = buildFilterCondition(filterConditions);
            resultJson.put("filter", filter);
        }
    }

    private void addProjection(JSONObject sourceParamObj, JSONObject resultJson) {
        JSONArray projectionFields = sourceParamObj.getJSONArray("projection");
        if (projectionFields != null && !projectionFields.isEmpty()) {
            JSONObject projection = buildProjection(projectionFields);
            resultJson.put("projection", projection);
        }
    }

    private JSONObject buildFilterCondition(JSONArray filterConditions) {
        JSONObject filter = new JSONObject();

        for (int i = 0; i < filterConditions.size(); i++) {
            JSONObject condition = filterConditions.getJSONObject(i);

            String field = validateField(condition, "field", i);
            String operator = validateField(condition, "operator", i);
            Object value = condition.get("value");

            String mongoOperator = mapOperatorToMongo(operator);
            JSONObject conditionObj = new JSONObject();
            conditionObj.put(mongoOperator, value);

            filter.put(field, conditionObj);
        }

        return filter;
    }

    private JSONObject buildProjection(JSONArray projectionFields) {
        JSONObject projection = new JSONObject();

        for (int i = 0; i < projectionFields.size(); i++) {
            String field = projectionFields.getString(i);
            if (StringUtils.isNotBlank(field)) {
                projection.put(field, 1);
            }
        }

        return projection;
    }

    private String mapOperatorToMongo(String operator) {
        switch (operator.toUpperCase()) {
            case "=":
                return "$eq";
            case ">":
                return "$gt";
            case ">=":
                return "$gte";
            case "<":
                return "$lt";
            case "<=":
                return "$lte";
            case "!=":
                return "$ne";
            case "IN":
                return "$in";
            case "LIKE":
                return "$regex";
            default:
                return "$eq";
        }
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

    private String getSinkCollection(JSONObject sourceObj, JSONObject sinkObj) {
        if (sinkObj.getBoolean("autoCreateTable")) {
            return sourceObj.getString("table_path");
        }
        return sinkObj.getString("table_path");
    }
}