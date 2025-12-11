package org.apache.cockpit.plugin.datasource.mongodb.catalog;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.spi.datasource.ConnectionParam;
import org.apache.cockpit.plugin.datasource.api.datasource.DataSourceProcessor;
import org.apache.cockpit.plugin.datasource.api.modal.DataSourceTableColumn;
import org.apache.cockpit.plugin.datasource.mongodb.param.MongoConnectionParam;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MongoMetadataService {

    public static List<String> listCollections(ConnectionParam connectionParam,
                                               DataSourceProcessor processor) {
        try {
            List<String> collections = new ArrayList<>();

            MongoConnectionParam mongoConnectionParam = (MongoConnectionParam) connectionParam;

            MongoClient mongoClient = MongoClients.create(mongoConnectionParam.getConnectionString());
            MongoDatabase database = mongoClient.getDatabase(mongoConnectionParam.getDatabase());
            for (String collectionName : database.listCollectionNames()) {
                collections.add(collectionName);
            }
            mongoClient.close();
            return collections;

        } catch (Exception e) {
            log.error("获取集合列表失败: {}", e.getMessage(), e);
            throw new RuntimeException("MongoDB操作失败: " + e.getMessage(), e);
        }
    }

    public static List<DataSourceTableColumn> listFields(ConnectionParam connectionParam,
                                                         String collectionName,
                                                         DataSourceProcessor processor) {
        List<DataSourceTableColumn> columns = new ArrayList<>();
        MongoConnectionParam mongoConnectionParam = (MongoConnectionParam) connectionParam;
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create(mongoConnectionParam.getConnectionString());
            MongoDatabase database = mongoClient.getDatabase(mongoConnectionParam.getDatabase());
            MongoCollection<Document> collection = database.getCollection(collectionName);
            Document sampleDoc = collection.find().first();
            if (sampleDoc != null) {
                for (String key : sampleDoc.keySet()) {
                    if (!key.equals("_id")) {
                        DataSourceTableColumn column = createColumnFromField(key, sampleDoc.get(key));
                        columns.add(column);
                    }
                }
            }
        } catch (Exception e) {
            log.error("获取集合 {} 的字段列表失败: {}", collectionName, e.getMessage(), e);
            throw new RuntimeException("获取字段失败: " + e.getMessage(), e);
        }
        return columns;
    }

    private static DataSourceTableColumn createColumnFromField(String fieldName, Object fieldValue) {
        String columnType = mapFieldType(fieldValue);

        return DataSourceTableColumn.builder()
                .columnName(fieldName)
                .columnType(columnType)
                .sourceType(columnType)
                .build();
    }

    private static String mapFieldType(Object fieldValue) {
        if (fieldValue == null) {
            return "STRING";
        }

        if (fieldValue instanceof Integer || fieldValue instanceof Long ||
                fieldValue instanceof Double || fieldValue instanceof Float) {
            return "NUMBER";
        } else if (fieldValue instanceof Boolean) {
            return "BOOLEAN";
        } else if (fieldValue instanceof java.util.Date) {
            return "TIMESTAMP";
        } else if (fieldValue instanceof java.util.List) {
            return "ARRAY";
        } else {
            return "STRING";
        }
    }
}
