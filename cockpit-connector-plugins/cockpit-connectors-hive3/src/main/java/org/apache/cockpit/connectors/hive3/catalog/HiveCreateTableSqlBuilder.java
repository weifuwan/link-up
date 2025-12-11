package org.apache.cockpit.connectors.hive3.catalog;

import org.apache.cockpit.connectors.api.catalog.*;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.hive3.dialect.HiveTypeConverter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HiveCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;
    private List<String> partitionKeys;
    private String comment;
    private String storedAs;
    private String rowFormat;
    private String location;
    private String tblProperties;
    private PrimaryKey primaryKey;
    private List<ConstraintKey> constraintKeys;
    private final HiveTypeConverter typeConverter;
    private boolean createIndex;

    private HiveCreateTableSqlBuilder(
            String tableName, HiveTypeConverter typeConverter, boolean createIndex) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.typeConverter = typeConverter;
        this.createIndex = createIndex;
    }

    public static HiveCreateTableSqlBuilder builder(
            TablePath tablePath,
            CatalogTable catalogTable,
            HiveTypeConverter typeConverter,
            boolean createIndex) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new HiveCreateTableSqlBuilder(tablePath.getTableName(), typeConverter, createIndex)
                .comment(catalogTable.getComment())
                .partitionKeys(catalogTable.getPartitionKeys())
                .storedAs(catalogTable.getOptions().getOrDefault("file_format_type", "TEXTFILE"))
                .rowFormat(catalogTable.getOptions().get("rowFormat"))
                .location(catalogTable.getOptions().get("location"))
                .tblProperties(buildTblProperties(catalogTable.getOptions()))
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns());
    }

    private static String buildTblProperties(Map<String, String> options) {
        if (options == null || options.isEmpty()) {
            return null;
        }

        List<String> tblProps = new ArrayList<>();
        // 排除已经在其他地方处理的属性
        String[] excludedProps = {"format", "rowFormat", "location"};

        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // 跳过已经处理的属性
            boolean skip = false;
            for (String excluded : excludedProps) {
                if (excluded.equalsIgnoreCase(key)) {
                    skip = true;
                    break;
                }
            }

            if (!skip && StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                tblProps.add(String.format("'%s'='%s'",
                        escapeHiveProperty(key),
                        escapeHiveProperty(value)));
            }
        }

        if (tblProps.isEmpty()) {
            return null;
        }

        return String.join(", ", tblProps);
    }

    private static String escapeHiveProperty(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "\\'").replace("\\", "\\\\");
    }

    public HiveCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public HiveCreateTableSqlBuilder partitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public HiveCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public HiveCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public HiveCreateTableSqlBuilder storedAs(String storedAs) {
        this.storedAs = storedAs;
        return this;
    }

    public HiveCreateTableSqlBuilder rowFormat(String rowFormat) {
        this.rowFormat = rowFormat;
        return this;
    }

    public HiveCreateTableSqlBuilder location(String location) {
        this.location = location;
        return this;
    }

    public HiveCreateTableSqlBuilder tblProperties(String tblProperties) {
        this.tblProperties = tblProperties;
        return this;
    }

    public HiveCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(String databaseName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE TABLE `")
                .append(databaseName)
                .append("`.`")
                .append(tableName)
                .append("` (\n");

        sqlBuilder.append(buildColumnsIdentifySql());

        sqlBuilder.append("\n)");

        if (StringUtils.isNotBlank(comment)) {
            sqlBuilder.append("\nCOMMENT '")
                    .append(escapeHiveComment(comment))
                    .append("'");
        }

        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            sqlBuilder.append("\nPARTITIONED BY (")
                    .append(buildPartitionColumnsSql())
                    .append(")");
        }

        if (StringUtils.isNotBlank(storedAs)) {
            String storedAsUpper = storedAs.toUpperCase();
            switch (storedAsUpper) {
                case "TEXTFILE":
                    sqlBuilder.append("row format delimited fields terminated by '\u0001' lines terminated by '\n' stored as textfile");
                    break;
                case "SEQUENCEFILE":
                    sqlBuilder.append("\nSTORED AS SEQUENCEFILE");
                    break;
                case "PARQUET":
                    sqlBuilder.append("\nSTORED AS PARQUET");
                    break;
                case "AVRO":
                    sqlBuilder.append("\nSTORED AS AVRO");
                    break;
                default:
                    sqlBuilder.append("\nSTORED AS ORC");
                    break;
            }
        } else {
            sqlBuilder.append("\nSTORED AS ORC");
        }
        return sqlBuilder.toString();
    }

    private String buildColumnsIdentifySql() {
        List<String> columnSqls = new ArrayList<>();
        Map<String, String> columnTypeMap = new HashMap<>();

        List<Column> nonPartitionColumns = columns;
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            nonPartitionColumns = columns.stream()
                    .filter(column -> !partitionKeys.contains(column.getName()))
                    .collect(Collectors.toList());
        }

        for (Column column : nonPartitionColumns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, columnTypeMap));
        }

        return String.join(",\n", columnSqls);
    }

    String buildColumnIdentifySql(Column column, Map<String, String> columnTypeMap) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add("`" + column.getName() + "`");
        BasicTypeDefine<String> typeDefine = typeConverter.reconvert(column);
        String type = typeDefine.getColumnType();

        columnSqls.add(type);
        columnTypeMap.put(column.getName(), type);

        if (!column.isNullable()) {
            columnSqls.add("NOT NULL");
        }

        if (StringUtils.isNotBlank(column.getComment())) {
            columnSqls.add("COMMENT '" + escapeHiveComment(column.getComment()) + "'");
        }

        return String.join(" ", columnSqls);
    }

    private String buildPartitionColumnsSql() {
        if (CollectionUtils.isEmpty(partitionKeys)) {
            return "";
        }

        List<String> partitionColumnSqls = new ArrayList<>();
        Map<String, Column> columnMap = columns.stream()
                .collect(Collectors.toMap(Column::getName, column -> column));

        for (String partitionKey : partitionKeys) {
            List<String> partitionColSql = new ArrayList<>();
            partitionColSql.add("`" + partitionKey + "`");

            Column partitionColumn = columnMap.get(partitionKey);

            String type = "string";

            partitionColSql.add(type);
            partitionColumnSqls.add(String.join(" ", partitionColSql));
        }

        return String.join(", ", partitionColumnSqls);
    }


    private String escapeHiveComment(String comment) {
        if (comment == null) {
            return "";
        }
        return comment.replace("'", "''").replace("\\", "\\\\");
    }

    // 构建索引创建语句（如果需要的话）
    public List<String> buildIndexSqls(String databaseName) {
        List<String> indexSqls = new ArrayList<>();

        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }

                ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
                if (constraintType == ConstraintKey.ConstraintType.INDEX_KEY) {
                    String indexSql = buildHiveIndexSql(databaseName, constraintKey);
                    if (StringUtils.isNotBlank(indexSql)) {
                        indexSqls.add(indexSql);
                    }
                } else if (constraintType == ConstraintKey.ConstraintType.UNIQUE_KEY) {
                }
            }
        }

        return indexSqls;
    }

    private String buildHiveIndexSql(String databaseName, ConstraintKey constraintKey) {
        if (CollectionUtils.isEmpty(constraintKey.getColumnNames())) {
            return null;
        }

        String indexColumns = constraintKey.getColumnNames().stream()
                .map(columnName -> "`" + columnName + "`")
                .collect(Collectors.joining(", "));

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("CREATE INDEX `")
                .append(constraintKey.getConstraintName())
                .append("` ON TABLE `")
                .append(databaseName)
                .append("`.`")
                .append(tableName)
                .append("` (")
                .append(indexColumns)
                .append(") AS 'COMPACT' WITH DEFERRED REBUILD");

        return sqlBuilder.toString();
    }
}
