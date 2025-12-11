package org.apache.cockpit.connectors.hive3.dialect;

import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveTypeMapper implements JdbcDialectTypeMapper {

    private HiveTypeConverter typeConverter;

    public HiveTypeMapper() {
        this(HiveTypeConverter.DEFAULT_INSTANCE);
    }

    public HiveTypeMapper(HiveTypeConverter typeConverter) {
        this.typeConverter = typeConverter;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return typeConverter.convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        String columnType = nativeType;
        int isNullable = metadata.isNullable(colIndex);
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        int displaySize = metadata.getColumnDisplaySize(colIndex);

        // Hive特有类型处理
        if ("DECIMAL".equalsIgnoreCase(nativeType)) {
            // Hive DECIMAL类型需要处理精度和范围
            if (precision <= 0) {
                precision = 10; // 默认精度
            }
            if (scale < 0) {
                scale = 0; // 默认范围
            }
            columnType = String.format("DECIMAL(%d,%d)", precision, scale);
        } else if ("VARCHAR".equalsIgnoreCase(nativeType) || "CHAR".equalsIgnoreCase(nativeType)) {
            // VARCHAR和CHAR类型需要处理长度
            if (precision <= 0 && displaySize > 0) {
                precision = displaySize;
            }
            if (precision > 0) {
                columnType = String.format("%s(%d)", nativeType.toUpperCase(), precision);
            }
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length((long) precision)
                        .precision((long) precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
