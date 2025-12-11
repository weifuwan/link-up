package org.apache.cockpit.plugin.datasource.mongodb.catalog;


import java.util.Arrays;
import java.util.List;

public class MongoQueryParser {

    public static List<String> splitAndRemoveComment(String query) {
        // MongoDB查询通常是JSON格式，不需要像SQL那样分割
        // 这里简单处理，直接返回整个查询
        String cleanQuery = removeComments(query);
        return Arrays.asList(cleanQuery);
    }

    private static String removeComments(String query) {
        // 简单的注释移除逻辑
        return query.replaceAll("//.*", "")  // 移除单行注释
                .replaceAll("/\\*.*?\\*/", "")  // 移除多行注释
                .trim();
    }
}
