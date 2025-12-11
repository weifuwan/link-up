package org.apache.cockpit.connectors.oracle.dialect;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.jdbc.config.JdbcOptions;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.JdbcDialectTypeMapper;
import org.apache.cockpit.connectors.api.util.TypeDefineUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
public class OracleTypeMapper implements JdbcDialectTypeMapper {

    private final boolean decimalTypeNarrowing;
    private final boolean handleBlobAsString;

    public OracleTypeMapper() {
        this(
                JdbcOptions.DECIMAL_TYPE_NARROWING.defaultValue(),
                JdbcOptions.HANDLE_BLOB_AS_STRING.defaultValue());
    }

    public OracleTypeMapper(boolean decimalTypeNarrowing) {
        this(decimalTypeNarrowing, JdbcOptions.HANDLE_BLOB_AS_STRING.defaultValue());
    }

    public OracleTypeMapper(boolean decimalTypeNarrowing, boolean handleBlobAsString) {
        this.decimalTypeNarrowing = decimalTypeNarrowing;
        this.handleBlobAsString = handleBlobAsString;
    }

    @Override
    public Column mappingColumn(BasicTypeDefine typeDefine) {
        return new OracleTypeConverter(decimalTypeNarrowing, handleBlobAsString)
                .convert(typeDefine);
    }

    @Override
    public Column mappingColumn(ResultSetMetaData metadata, int colIndex) throws SQLException {
        String columnName = metadata.getColumnLabel(colIndex);
        String nativeType = metadata.getColumnTypeName(colIndex);
        int isNullable = metadata.isNullable(colIndex);
        long precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        if ("number".equalsIgnoreCase(nativeType) && scale == -127) {
            nativeType = "float";
        } else if (Arrays.asList("NVARCHAR2", "NCHAR").contains(nativeType)) {
            long doubleByteLength = TypeDefineUtils.charToDoubleByteLength(precision);
            precision = doubleByteLength;
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(nativeType)
                        .dataType(nativeType)
                        .nullable(isNullable == ResultSetMetaData.columnNullable)
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();
        return mappingColumn(typeDefine);
    }
}
