package org.apache.cockpit.plugin.datasource.mysql.catalog;

import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class MySQLQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }
}
