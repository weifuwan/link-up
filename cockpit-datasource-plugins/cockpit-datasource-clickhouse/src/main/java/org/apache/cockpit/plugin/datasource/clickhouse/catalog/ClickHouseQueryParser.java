package org.apache.cockpit.plugin.datasource.clickhouse.catalog;


import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class ClickHouseQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        // ClickHouse语法与MySQL类似，使用MySQL解析器
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }
}