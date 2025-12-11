package org.apache.cockpit.plugin.datasource.dm.catalog;

import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class DmQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.dm);
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.dm);
    }
}
