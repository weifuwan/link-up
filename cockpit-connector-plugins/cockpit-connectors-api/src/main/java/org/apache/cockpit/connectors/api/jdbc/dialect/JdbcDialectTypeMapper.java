package org.apache.cockpit.connectors.api.jdbc.dialect;


import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;

import java.io.Serializable;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.sql.Types.*;

/** Separate the jdbc meta-information type to SeaTunnelDataType into the interface. */
public interface JdbcDialectTypeMapper extends Serializable {

    /**
     * @deprecated instead by {@link #mappingColumn(BasicTypeDefine)}
     * @param metadata
     * @param colIndex
     * @return
     * @throws SQLException
     */
    @Deprecated
    default SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .sqlType(metadata.getColumnType(colIndex))
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length((long) precision)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine).getDataType();
    }

    default Column mappingColumn(BasicTypeDefine typeDefine) {
        throw new UnsupportedOperationException();
    }

    default List<Column> mappingColumn(
            DatabaseMetaData metadata,
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String columnNamePattern)
            throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (ResultSet rs =
                metadata.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String nativeType = rs.getString("TYPE_NAME");
                int sqlType = rs.getInt("DATA_TYPE");
                int columnSize = rs.getInt("COLUMN_SIZE");
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                int nullable = rs.getInt("NULLABLE");
                String comment = rs.getString("REMARKS");

                BasicTypeDefine typeDefine =
                        BasicTypeDefine.builder()
                                .name(columnName)
                                .columnType(nativeType)
                                .dataType(nativeType)
                                .sqlType(sqlType)
                                .length((long) columnSize)
                                .precision((long) columnSize)
                                .scale(decimalDigits)
                                .nullable(nullable == DatabaseMetaData.columnNullable)
                                .comment(comment)
                                .build();
                columns.add(mappingColumn(typeDefine));
            }
        }
        return columns;
    }

    default List<Column> mappingColumn(ResultSetMetaData metadata) throws SQLException {
        List<Column> columns = new ArrayList<>();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
            Column column = mappingColumn(metadata, index);
            columns.add(column);
        }
        return columns;
    }

    default Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        /**
         * TODO The mapping method should be replaced by {@link #mappingColumn(BasicTypeDefine)}.
         */
        SeaTunnelDataType seaTunnelType = mapping(metadata, colIndex);

        String columnName = metadata.getColumnLabel(colIndex);
        int jdbcType = metadata.getColumnType(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);

        int columnLength = precision;
        long longColumnLength = precision;
        long bitLength = 0;
        switch (jdbcType) {
            case BINARY:
            case VARBINARY:
            case LONGVARBINARY:
            case BLOB:
                bitLength = precision * 8;
                break;
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
            case NCHAR:
            case NVARCHAR:
            case LONGNVARCHAR:
            case CLOB:
            case NCLOB:
                columnLength = precision * 3;
                longColumnLength = precision * 3;
                break;
            default:
                break;
        }

        return PhysicalColumn.of(
                columnName,
                seaTunnelType,
                columnLength,
                isNullable != ResultSetMetaData.columnNoNulls,
                null,
                null,
                nativeType,
                false,
                false,
                bitLength,
                Collections.emptyMap(),
                longColumnLength);
    }
}
