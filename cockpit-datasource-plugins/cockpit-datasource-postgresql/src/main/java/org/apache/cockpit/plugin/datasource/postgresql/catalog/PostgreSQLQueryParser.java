package org.apache.cockpit.plugin.datasource.postgresql.catalog;


import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class PostgreSQLQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.postgresql);
        return splitSqlRespectingDollarQuotes(cleanSQL);
    }

    private static List<String> splitSqlRespectingDollarQuotes(String sql) {
        return SQLParserUtils.splitAndRemoveComment(sql, com.alibaba.druid.DbType.postgresql);
    }
}
