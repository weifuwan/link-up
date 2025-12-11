package org.apache.cockpit.plugin.datasource.cache.catalog;


import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class CacheQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        // Cache数据库使用MySQL语法解析器
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }
}
