package org.apache.cockpit.connectors.db2.catalog;

import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PrimaryKey;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.util.CatalogUtils;
import org.apache.cockpit.connectors.db2.dialect.DB2TypeConverter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Henry
 * @date 2025/12/4
 */
public class DB2CreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private String sourceCatalogName;
    private String fieldIde;
    private boolean createIndex;

    public DB2CreateTableSqlBuilder(CatalogTable catalogTable, boolean createIndex) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
//        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        // TODO
        this.fieldIde = "uppercase";
        this.createIndex = createIndex;
    }

    public List<String> build(TablePath tablePath) {
        List<String> sqls = new ArrayList<>();
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(column -> CatalogUtils.getFieldIde(buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        // Add primary key directly in the create table statement
        if (createIndex
                && primaryKey != null
                && primaryKey.getColumnNames() != null
                && primaryKey.getColumnNames().size() > 0) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");
        sqls.add(createTableSql.toString());
        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                column ->
                                        buildColumnCommentSql(
                                                column, tablePath.getSchemaAndTableName("\"")))
                        .collect(Collectors.toList());
        sqls.addAll(commentSqls);
        return sqls;
    }

    String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        String columnName = column.getName();
        columnSql.append("\"").append(columnName).append("\" ");

        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (StringUtils.equalsIgnoreCase(DatabaseIdentifier.DB_2, sourceCatalogName)
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = DB2TypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));
        // In DB2 database, the maximum length for an identifier is 128 characters, but we keep 25 for consistency
        String primaryKeyStr = primaryKey.getPrimaryKey();
        if (primaryKeyStr.length() > 25) {
            primaryKeyStr = primaryKeyStr.substring(0, 25);
        }

        return CatalogUtils.getFieldIde(
                "CONSTRAINT "
                        + primaryKeyStr
                        + "_"
                        + randomSuffix
                        + " PRIMARY KEY ("
                        + columnNamesString
                        + ")",
                fieldIde);
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier("COMMENT ON COLUMN ", fieldIde))
                .append(tableName)
                .append(".");
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "\""))
                .append(CatalogUtils.quoteIdentifier(" IS '", fieldIde))
                .append(column.getComment().replace("'", "''"))
                .append("'");
        return columnCommentSql.toString();
    }
}
