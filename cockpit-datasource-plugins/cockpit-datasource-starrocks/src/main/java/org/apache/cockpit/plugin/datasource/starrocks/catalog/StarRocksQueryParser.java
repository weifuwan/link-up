package org.apache.cockpit.plugin.datasource.starrocks.catalog;



import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class StarRocksQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.mysql); // StarRocks语法兼容MySQL
        return SQLParserUtils.split(cleanSQL, com.alibaba.druid.DbType.mysql);
    }
}
