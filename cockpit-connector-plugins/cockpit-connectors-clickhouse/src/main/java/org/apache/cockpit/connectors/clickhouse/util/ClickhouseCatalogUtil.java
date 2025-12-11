package org.apache.cockpit.connectors.clickhouse.util;

import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.TablePath;
import org.apache.cockpit.connectors.api.util.CatalogUtil;
import org.apache.cockpit.connectors.clickhouse.catalog.ClickhouseTypeConverter;
import org.apache.commons.lang3.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClickhouseCatalogUtil extends CatalogUtil {

    public static final ClickhouseCatalogUtil INSTANCE = new ClickhouseCatalogUtil();

    public String columnToConnectorType(Column column) {
        checkNotNull(column, "The column is required.");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else {
            columnType = ClickhouseTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }

        // If the field is nullable and the type does not start with `Nullable(`, then wrap it as a `Nullable` type.
        if (column.isNullable() && !columnType.startsWith("Nullable(")) {
            columnType = "Nullable(" + columnType + ")";
        }

        return String.format(
                "`%s` %s %s",
                column.getName(),
                columnType,
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '"
                        + column.getComment().replace("'", "''").replace("\\", "\\\\")
                        + "'");
    }

    public String getDropTableSql(TablePath tablePath, boolean ignoreIfNotExists) {
        if (ignoreIfNotExists) {
            return "DROP TABLE IF EXISTS "
                    + tablePath.getDatabaseName()
                    + "."
                    + tablePath.getTableName();
        } else {
            return "DROP TABLE " + tablePath.getDatabaseName() + "." + tablePath.getTableName();
        }
    }

    public String getTruncateTableSql(TablePath tablePath) {
        return "TRUNCATE TABLE " + tablePath.getDatabaseName() + "." + tablePath.getTableName();
    }
}
