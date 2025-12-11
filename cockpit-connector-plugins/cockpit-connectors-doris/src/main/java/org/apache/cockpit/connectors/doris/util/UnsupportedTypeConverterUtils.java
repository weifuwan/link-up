package org.apache.cockpit.connectors.doris.util;


import org.apache.cockpit.connectors.api.catalog.CatalogTable;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.catalog.TableSchema;
import org.apache.cockpit.connectors.api.type.DecimalType;
import org.apache.cockpit.connectors.api.type.SeaTunnelRow;
import org.apache.cockpit.connectors.api.type.SqlType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.cockpit.connectors.api.type.BasicType.DOUBLE_TYPE;


public class UnsupportedTypeConverterUtils {

    public static Object convertBigDecimal(BigDecimal bigDecimal) {
        if (bigDecimal.precision() > 38) {
            return bigDecimal.doubleValue();
        }
        return bigDecimal;
    }

    public static SeaTunnelRow convertRow(SeaTunnelRow row) {
        List<Object> newValues =
                Arrays.stream(row.getFields())
                        .map(
                                value -> {
                                    if (value instanceof BigDecimal) {
                                        return convertBigDecimal((BigDecimal) value);
                                    }
                                    return value;
                                })
                        .collect(Collectors.toList());
        return new SeaTunnelRow(newValues.toArray());
    }

    public static CatalogTable convertCatalogTable(CatalogTable catalogTable) {
        TableSchema tableSchema = catalogTable.getTableSchema();
        List<Column> columns = tableSchema.getColumns();
        List<Column> newColumns =
                columns.stream()
                        .map(
                                column -> {
                                    if (column.getDataType().getSqlType().equals(SqlType.DECIMAL)) {
                                        DecimalType decimalType =
                                                (DecimalType) column.getDataType();
                                        if (decimalType.getPrecision() > 38) {
                                            return PhysicalColumn.of(
                                                    column.getName(),
                                                    DOUBLE_TYPE,
                                                    22,
                                                    column.isNullable(),
                                                    null,
                                                    column.getComment(),
                                                    "DOUBLE",
                                                    false,
                                                    false,
                                                    0L,
                                                    column.getOptions(),
                                                    22L);
                                        }
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());
        TableSchema newtableSchema =
                TableSchema.builder()
                        .columns(newColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                newtableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }
}
