package org.apache.cockpit.plugin.datasource.hive.catalog;



import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class HiveQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.hive);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.hive);
    }
}
