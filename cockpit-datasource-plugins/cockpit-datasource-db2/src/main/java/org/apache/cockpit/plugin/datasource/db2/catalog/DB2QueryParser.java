package org.apache.cockpit.plugin.datasource.db2.catalog;

import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class DB2QueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.db2);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.db2);
    }
}
