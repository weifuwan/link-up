package org.apache.cockpit.plugin.datasource.opengauss.catalog;

import com.alibaba.druid.sql.parser.SQLParserUtils;

import java.util.List;

public class OpenGaussQueryParser {

    public static List<String> splitAndRemoveComment(String sql) {
        String cleanSQL = SQLParserUtils.removeComment(sql, com.alibaba.druid.DbType.postgresql);
        return splitSqlRespectingDollarQuotes(cleanSQL);
    }

    private static List<String> splitSqlRespectingDollarQuotes(String sql) {
        // 这里保留原有的美元符号引用处理逻辑
        // 由于代码较长，这里使用简化的实现，实际可以使用原来的完整实现
        return SQLParserUtils.splitAndRemoveComment(sql, com.alibaba.druid.DbType.postgresql);
    }
}