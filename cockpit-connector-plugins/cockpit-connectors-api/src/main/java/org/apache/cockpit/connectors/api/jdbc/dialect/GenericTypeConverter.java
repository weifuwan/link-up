package org.apache.cockpit.connectors.api.jdbc.dialect;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.type.BasicType;
import org.apache.cockpit.connectors.api.type.DecimalType;
import org.apache.cockpit.connectors.api.type.LocalTimeType;
import org.apache.cockpit.connectors.api.type.PrimitiveByteArrayType;

import java.sql.Types;

@Slf4j
@AutoService(TypeConverter.class)
public class GenericTypeConverter implements TypeConverter<BasicTypeDefine> {

    public static final GenericTypeConverter DEFAULT_INSTANCE = new GenericTypeConverter();

    public static final int MAX_PRECISION = 65;
    public static final int DEFAULT_PRECISION = 38;
    public static final int MAX_SCALE = MAX_PRECISION - 1;
    public static final int DEFAULT_SCALE = 18;

    @Override
    public String identifier() {
        return DatabaseIdentifier.GENERIC;
    }

    /**
     * Convert an external system's type definition to {@link Column}.
     *
     * @param typeDefine type define
     * @return column
     */
    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        int sqlType = typeDefine.getSqlType();
        switch (sqlType) {
            case Types.NULL:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case Types.BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case Types.BIT:
                if (typeDefine.getLength() == null
                        || typeDefine.getLength() <= 0
                        || typeDefine.getLength() == 1) {
                    builder.dataType(BasicType.BOOLEAN_TYPE);
                } else {
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    // BIT(M) -> BYTE(M/8)
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                }
                break;
            case Types.TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case Types.SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case Types.INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case Types.BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case Types.REAL:
            case Types.FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case Types.DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case Types.NUMERIC:
                DecimalType decimalTypeForNumeric;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    decimalTypeForNumeric =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(), typeDefine.getScale());
                } else {
                    decimalTypeForNumeric = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                }
                builder.dataType(decimalTypeForNumeric);
                break;
            case Types.DECIMAL:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);
                DecimalType decimalType;
                if (typeDefine.getPrecision() > DEFAULT_PRECISION) {
                    decimalType = new DecimalType(DEFAULT_PRECISION, DEFAULT_SCALE);
                } else {
                    decimalType =
                            new DecimalType(
                                    typeDefine.getPrecision().intValue(),
                                    typeDefine.getScale() == null
                                            ? 0
                                            : typeDefine.getScale().intValue());
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.DATALINK:
            case Types.NCLOB:
            case Types.SQLXML:
                builder.dataType(BasicType.STRING_TYPE);
                break;

            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(1L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                break;
            case Types.DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case Types.TIME:
                builder.dataType(LocalTimeType.LOCAL_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case Types.TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;

            case Types.OTHER:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default:
                log.warn(
                        "JDBC type {} ({}) not currently supported",
                        sqlType,
                        typeDefine.getNativeType());
        }
        return builder.build();
    }

    /**
     * Convert {@link Column} to an external system's type definition.
     *
     * @param column
     * @return
     */
    @Override
    public BasicTypeDefine reconvert(Column column) {
        throw new UnsupportedOperationException(
                String.format(
                        "%s (%s) type doesn't have a mapping to the SQL database column type",
                        column.getName(), column.getDataType().getSqlType().name()));
    }
}
