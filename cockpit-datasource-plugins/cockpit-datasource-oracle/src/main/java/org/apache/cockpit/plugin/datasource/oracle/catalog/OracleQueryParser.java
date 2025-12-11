package org.apache.cockpit.plugin.datasource.oracle.catalog;


import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.oracle.parser.OracleStatementParser;
import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;
import java.util.stream.Collectors;

public class OracleQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        if (sql.toUpperCase().contains("BEGIN") && sql.toUpperCase().contains("END")) {
            return new OracleStatementParser(sql).parseStatementList().stream().map(SQLStatement::toString)
                    .collect(Collectors.toList());
        }
        return SQLParserUtils.splitAndRemoveComment(sql, com.alibaba.druid.DbType.oracle);
    }
}
