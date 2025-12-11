package org.apache.cockpit.plugin.datasource.doris.catalog;


import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class DorisQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        // Because doris is highly compatible with mysql syntax,
        // a mysql-type implementation can be used for comments and statement split.
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }
}
