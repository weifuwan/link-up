package org.apache.cockpit.plugin.datasource.sqlserver.catalog;

import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;
import java.util.stream.Collectors;

public class SQLServerQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        return SQLParserUtils.splitAndRemoveComment(sql, com.alibaba.druid.DbType.sqlserver)
                .stream()
                .map(subSql -> subSql.concat(";"))
                .collect(Collectors.toList());
    }
}
