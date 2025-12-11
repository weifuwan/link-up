package org.apache.cockpit.connectors.hive3.dialect;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.type.BasicType;
import org.apache.cockpit.connectors.api.type.DecimalType;
import org.apache.cockpit.connectors.api.type.LocalTimeType;
import org.apache.cockpit.connectors.api.type.PrimitiveByteArrayType;

// reference https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
@Slf4j
@AutoService(TypeConverter.class)
public class HiveTypeConverter implements TypeConverter<BasicTypeDefine<String>> {

    // ============================data types=====================
    static final String HIVE_VOID = "VOID";
    static final String HIVE_BOOLEAN = "BOOLEAN";

    // -------------------------numeric types---------------------
    static final String HIVE_TINYINT = "TINYINT";
    static final String HIVE_SMALLINT = "SMALLINT";
    static final String HIVE_INT = "INT";
    static final String HIVE_INTEGER = "INTEGER";
    static final String HIVE_BIGINT = "BIGINT";
    static final String HIVE_FLOAT = "FLOAT";
    static final String HIVE_DOUBLE = "DOUBLE";
    static final String HIVE_DECIMAL = "DECIMAL";

    // -------------------------date/time types-------------------
    static final String HIVE_DATE = "DATE";
    static final String HIVE_TIMESTAMP = "TIMESTAMP";
    static final String HIVE_INTERVAL = "INTERVAL";

    // -------------------------string types----------------------
    static final String HIVE_STRING = "STRING";
    static final String HIVE_VARCHAR = "VARCHAR";
    static final String HIVE_CHAR = "CHAR";

    // -------------------------misc types------------------------
    static final String HIVE_BINARY = "BINARY";

    // -------------------------complex types---------------------
    static final String HIVE_ARRAY = "ARRAY";
    static final String HIVE_MAP = "MAP";
    static final String HIVE_STRUCT = "STRUCT";
    static final String HIVE_UNION = "UNIONTYPE";

    public static final int DEFAULT_DECIMAL_PRECISION = 10;
    public static final int MAX_DECIMAL_PRECISION = 38;
    public static final int DEFAULT_DECIMAL_SCALE = 0;
    public static final int MAX_DECIMAL_SCALE = 38;
    public static final int DEFAULT_VARCHAR_LENGTH = 65535;
    public static final int MAX_VARCHAR_LENGTH = 65535;
    public static final int DEFAULT_CHAR_LENGTH = 255;
    public static final int MAX_CHAR_LENGTH = 255;

    public static final HiveTypeConverter DEFAULT_INSTANCE = new HiveTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.HIVE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());

        String hiveDataType = typeDefine.getDataType().toUpperCase();

        switch (hiveDataType) {
            case HIVE_VOID:
                builder.dataType(BasicType.VOID_TYPE);
                break;
            case HIVE_BOOLEAN:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case HIVE_TINYINT:
                builder.dataType(BasicType.BYTE_TYPE);
                break;
            case HIVE_SMALLINT:
                builder.dataType(BasicType.SHORT_TYPE);
                break;
            case HIVE_INT:
            case HIVE_INTEGER:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case HIVE_BIGINT:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case HIVE_FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case HIVE_DOUBLE:
                builder.dataType(BasicType.DOUBLE_TYPE);
                break;
            case HIVE_DECIMAL:
                Preconditions.checkArgument(typeDefine.getPrecision() > 0);

                DecimalType decimalType;
                if (typeDefine.getPrecision() > MAX_DECIMAL_PRECISION) {
                    log.warn("Decimal precision {} exceeds maximum {}, using maximum value",
                            typeDefine.getPrecision(), MAX_DECIMAL_PRECISION);
                    decimalType = new DecimalType(MAX_DECIMAL_PRECISION,
                            typeDefine.getScale() == null ? 0 : typeDefine.getScale().intValue());
                } else {
                    decimalType = new DecimalType(
                            typeDefine.getPrecision().intValue(),
                            typeDefine.getScale() == null ? 0 : typeDefine.getScale().intValue());
                }
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(decimalType.getPrecision()));
                builder.scale(decimalType.getScale());
                break;
            case HIVE_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case HIVE_TIMESTAMP:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale());
                break;
            case HIVE_CHAR:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(Long.valueOf(DEFAULT_CHAR_LENGTH));
                } else if (typeDefine.getLength() > MAX_CHAR_LENGTH) {
                    log.warn("CHAR length {} exceeds maximum {}, using maximum value",
                            typeDefine.getLength(), MAX_CHAR_LENGTH);
                    builder.columnLength(Long.valueOf(MAX_CHAR_LENGTH));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case HIVE_VARCHAR:
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(Long.valueOf(DEFAULT_VARCHAR_LENGTH));
                } else if (typeDefine.getLength() > MAX_VARCHAR_LENGTH) {
                    log.warn("VARCHAR length {} exceeds maximum {}, using maximum value",
                            typeDefine.getLength(), MAX_VARCHAR_LENGTH);
                    builder.columnLength(Long.valueOf(MAX_VARCHAR_LENGTH));
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case HIVE_STRING:
                builder.dataType(BasicType.STRING_TYPE);
                // Hive STRING type has no length limit, but we set a reasonable default
                builder.columnLength(65535L);
                break;
            case HIVE_BINARY:
                builder.dataType(PrimitiveByteArrayType.INSTANCE);
                if (typeDefine.getLength() == null || typeDefine.getLength() <= 0) {
                    builder.columnLength(256L);
                } else {
                    builder.columnLength(typeDefine.getLength());
                }
                break;
            case HIVE_ARRAY:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case HIVE_MAP:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            case HIVE_STRUCT:
                builder.dataType(BasicType.STRING_TYPE);
                break;
            default:
                if (hiveDataType.startsWith(HIVE_DECIMAL)) {
                    handleDecimalType(typeDefine, builder, hiveDataType);
                } else if (hiveDataType.startsWith(HIVE_VARCHAR)) {
                    handleVarcharType(typeDefine, builder, hiveDataType);
                } else if (hiveDataType.startsWith(HIVE_CHAR)) {
                    handleCharType(typeDefine, builder, hiveDataType);
                } else {
                    log.warn("Unsupported Hive type: {}, treating as STRING", hiveDataType);
                    builder.dataType(BasicType.STRING_TYPE);
                    builder.columnLength(65535L);
                }
        }
        return builder.build();
    }

    private void handleDecimalType(BasicTypeDefine typeDefine,
                                   PhysicalColumn.PhysicalColumnBuilder builder,
                                   String hiveDataType) {
        try {
            String typeStr = hiveDataType.replace(HIVE_DECIMAL, "").trim();
            if (typeStr.startsWith("(") && typeStr.endsWith(")")) {
                typeStr = typeStr.substring(1, typeStr.length() - 1);
                String[] parts = typeStr.split(",");
                if (parts.length == 2) {
                    int precision = Integer.parseInt(parts[0].trim());
                    int scale = Integer.parseInt(parts[1].trim());

                    if (precision > MAX_DECIMAL_PRECISION) {
                        log.warn("Decimal precision {} exceeds maximum {}, using maximum value",
                                precision, MAX_DECIMAL_PRECISION);
                        precision = MAX_DECIMAL_PRECISION;
                    }
                    if (scale > MAX_DECIMAL_SCALE) {
                        log.warn("Decimal scale {} exceeds maximum {}, using maximum value",
                                scale, MAX_DECIMAL_SCALE);
                        scale = MAX_DECIMAL_SCALE;
                    }

                    DecimalType decimalType = new DecimalType(precision, scale);
                    builder.dataType(decimalType);
                    builder.columnLength(Long.valueOf(precision));
                    builder.scale(scale);
                } else {
                    throw new IllegalArgumentException("Invalid decimal format: " + hiveDataType);
                }
            } else {
                DecimalType decimalType = new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
                builder.dataType(decimalType);
                builder.columnLength(Long.valueOf(DEFAULT_DECIMAL_PRECISION));
                builder.scale(DEFAULT_DECIMAL_SCALE);
            }
        } catch (Exception e) {
            log.warn("Failed to parse decimal type: {}, using default decimal(10,0)", hiveDataType, e);
            DecimalType decimalType = new DecimalType(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
            builder.dataType(decimalType);
            builder.columnLength(Long.valueOf(DEFAULT_DECIMAL_PRECISION));
            builder.scale(DEFAULT_DECIMAL_SCALE);
        }
    }

    private void handleVarcharType(BasicTypeDefine typeDefine,
                                   PhysicalColumn.PhysicalColumnBuilder builder,
                                   String hiveDataType) {
        try {
            String typeStr = hiveDataType.replace(HIVE_VARCHAR, "").trim();
            if (typeStr.startsWith("(") && typeStr.endsWith(")")) {
                typeStr = typeStr.substring(1, typeStr.length() - 1);
                long length = Long.parseLong(typeStr.trim());

                if (length > MAX_VARCHAR_LENGTH) {
                    log.warn("VARCHAR length {} exceeds maximum {}, using maximum value",
                            length, MAX_VARCHAR_LENGTH);
                    length = MAX_VARCHAR_LENGTH;
                }

                builder.columnLength(length);
                builder.dataType(BasicType.STRING_TYPE);
            } else {
                builder.columnLength(Long.valueOf(DEFAULT_VARCHAR_LENGTH));
                builder.dataType(BasicType.STRING_TYPE);
            }
        } catch (Exception e) {
            log.warn("Failed to parse varchar type: {}, using default varchar(65535)", hiveDataType, e);
            builder.columnLength(Long.valueOf(DEFAULT_VARCHAR_LENGTH));
            builder.dataType(BasicType.STRING_TYPE);
        }
    }

    private void handleCharType(BasicTypeDefine typeDefine,
                                PhysicalColumn.PhysicalColumnBuilder builder,
                                String hiveDataType) {
        try {
            String typeStr = hiveDataType.replace(HIVE_CHAR, "").trim();
            if (typeStr.startsWith("(") && typeStr.endsWith(")")) {
                typeStr = typeStr.substring(1, typeStr.length() - 1);
                long length = Long.parseLong(typeStr.trim());

                if (length > MAX_CHAR_LENGTH) {
                    log.warn("CHAR length {} exceeds maximum {}, using maximum value",
                            length, MAX_CHAR_LENGTH);
                    length = MAX_CHAR_LENGTH;
                }

                builder.columnLength(length);
                builder.dataType(BasicType.STRING_TYPE);
            } else {
                builder.columnLength(Long.valueOf(DEFAULT_CHAR_LENGTH));
                builder.dataType(BasicType.STRING_TYPE);
            }
        } catch (Exception e) {
            log.warn("Failed to parse char type: {}, using default char(255)", hiveDataType, e);
            builder.columnLength(Long.valueOf(DEFAULT_CHAR_LENGTH));
            builder.dataType(BasicType.STRING_TYPE);
        }
    }

    @Override
    public BasicTypeDefine<String> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder<String> builder =
                BasicTypeDefine.<String>builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case NULL:
                builder.columnType(HIVE_VOID);
                builder.dataType(HIVE_VOID);
                break;
            case BOOLEAN:
                builder.columnType(HIVE_BOOLEAN);
                builder.dataType(HIVE_BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(HIVE_TINYINT);
                builder.dataType(HIVE_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(HIVE_SMALLINT);
                builder.dataType(HIVE_SMALLINT);
                break;
            case INT:
                builder.columnType(HIVE_INT);
                builder.dataType(HIVE_INT);
                break;
            case BIGINT:
                builder.columnType(HIVE_BIGINT);
                builder.dataType(HIVE_BIGINT);
                break;
            case FLOAT:
                builder.columnType(HIVE_FLOAT);
                builder.dataType(HIVE_FLOAT);
                break;
            case DOUBLE:
                builder.columnType(HIVE_DOUBLE);
                builder.dataType(HIVE_DOUBLE);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                int precision = decimalType.getPrecision();
                int scale = decimalType.getScale();

                if (precision <= 0) {
                    precision = DEFAULT_DECIMAL_PRECISION;
                    scale = DEFAULT_DECIMAL_SCALE;
                    log.warn("Decimal precision must be > 0, using default decimal({},{})",
                            precision, scale);
                } else if (precision > MAX_DECIMAL_PRECISION) {
                    scale = Math.max(0, scale - (precision - MAX_DECIMAL_PRECISION));
                    precision = MAX_DECIMAL_PRECISION;
                    log.warn("Decimal precision exceeds maximum {}, using decimal({},{})",
                            MAX_DECIMAL_PRECISION, precision, scale);
                }

                if (scale < 0) {
                    scale = 0;
                    log.warn("Decimal scale must be >= 0, setting to 0");
                } else if (scale > MAX_DECIMAL_SCALE) {
                    scale = MAX_DECIMAL_SCALE;
                    log.warn("Decimal scale exceeds maximum {}, using {}",
                            MAX_DECIMAL_SCALE, scale);
                }

                if (scale > precision) {
                    log.warn("Decimal scale {} exceeds precision {}, adjusting precision to {}",
                            scale, precision, scale + 1);
                    precision = scale + 1;
                    if (precision > MAX_DECIMAL_PRECISION) {
                        precision = MAX_DECIMAL_PRECISION;
                        scale = Math.min(scale, MAX_DECIMAL_SCALE);
                    }
                }

                builder.columnType(String.format("%s(%d,%d)", HIVE_DECIMAL, precision, scale));
                builder.dataType(HIVE_DECIMAL);
                builder.precision((long) precision);
                builder.scale(scale);
                break;
            case BYTES:
                builder.columnType(HIVE_BINARY);
                builder.dataType(HIVE_BINARY);
                if (column.getColumnLength() != null && column.getColumnLength() > 0) {
                    builder.length(column.getColumnLength());
                }
                break;
            case STRING:
                if (column.getColumnLength() != null && column.getColumnLength() > 0) {
                    if (column.getColumnLength() <= MAX_CHAR_LENGTH) {
                        builder.columnType(String.format("%s(%d)", HIVE_CHAR, column.getColumnLength()));
                        builder.dataType(HIVE_CHAR);
                        builder.length(column.getColumnLength());
                    } else if (column.getColumnLength() <= MAX_VARCHAR_LENGTH) {
                        builder.columnType(String.format("%s(%d)", HIVE_VARCHAR, column.getColumnLength()));
                        builder.dataType(HIVE_VARCHAR);
                        builder.length(column.getColumnLength());
                    } else {
                        builder.columnType(HIVE_STRING);
                        builder.dataType(HIVE_STRING);
                    }
                } else {
                    builder.columnType(HIVE_STRING);
                    builder.dataType(HIVE_STRING);
                }
                break;
            case DATE:
                builder.columnType(HIVE_DATE);
                builder.dataType(HIVE_DATE);
                break;
            case TIMESTAMP:
                builder.columnType(HIVE_STRING);
                builder.dataType(HIVE_STRING);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.HIVE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }

        return builder.build();
    }
}