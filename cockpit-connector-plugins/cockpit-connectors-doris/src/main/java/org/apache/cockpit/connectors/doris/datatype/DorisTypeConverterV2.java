package org.apache.cockpit.connectors.doris.datatype;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.type.*;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.cockpit.connectors.doris.config.DorisBaseOptions.IDENTIFIER;


/**
 * Doris type converter for version 2.x
 */
@Slf4j
@AutoService(TypeConverter.class)
public class DorisTypeConverterV2 extends AbstractDorisTypeConverter {

    public static final String DORIS_ARRAY = "ARRAY";

    public static final String DORIS_ARRAY_BOOLEAN_INTER = "tinyint(1)";
    public static final String DORIS_ARRAY_TINYINT_INTER = "tinyint(4)";
    public static final String DORIS_ARRAY_SMALLINT_INTER = "smallint(6)";
    public static final String DORIS_ARRAY_INT_INTER = "int(11)";
    public static final String DORIS_ARRAY_BIGINT_INTER = "bigint(20)";
    public static final String DORIS_ARRAY_DECIMAL_PRE = "DECIMAL";
    public static final String DORIS_ARRAY_DATE_INTER = "date";
    public static final String DORIS_ARRAY_DATEV2_INTER = "DATEV2";
    public static final String DORIS_ARRAY_DATETIME_INTER = "DATETIME";
    public static final String DORIS_ARRAY_DATETIMEV2_INTER = "DATETIMEV2";

    public static final String DORIS_MAP = "MAP";
    public static final String DORIS_MAP_COLUMN_TYPE = "MAP<%s, %s>";

    public static final DorisTypeConverterV2 INSTANCE = new DorisTypeConverterV2();

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        return convert(typeDefine, true);
    }

    public Column convert(BasicTypeDefine typeDefine, boolean caseSensitive) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                getPhysicalColumnBuilder(typeDefine, caseSensitive);
        String dorisColumnType = getDorisColumnName(typeDefine);

        switch (dorisColumnType) {
            case DORIS_DATE:
                builder.dataType(LocalTimeType.LOCAL_DATE_TYPE);
                break;
            case DORIS_DATETIME:
                builder.dataType(LocalTimeType.LOCAL_DATE_TIME_TYPE);
                builder.scale(typeDefine.getScale() == null ? 0 : typeDefine.getScale());
                break;
            case DORIS_DECIMALV3:
                Long p = MAX_PRECISION;
                int scale = MAX_SCALE;
                if (typeDefine.getPrecision() != null && typeDefine.getPrecision() > 0) {
                    p = typeDefine.getPrecision();
                }

                if (typeDefine.getScale() != null && typeDefine.getScale() > 0) {
                    scale = typeDefine.getScale();
                }
                DecimalType decimalType;
                decimalType = new DecimalType(p.intValue(), scale);
                builder.dataType(decimalType);
                builder.columnLength(p);
                builder.scale(scale);
                break;
            case DORIS_ARRAY:
                convertArray(typeDefine.getColumnType(), builder, typeDefine.getName());
                break;
            case DORIS_MAP:
                convertMap(typeDefine.getColumnType(), builder, typeDefine.getName());
                break;
            default:
                super.sampleTypeConverter(builder, typeDefine, dorisColumnType);
        }

        return builder.build();
    }

    private void convertMap(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String[] keyValueType = extractMapKeyValueType(columnType);
        MapType mapType =
                new MapType(
                        turnColumnTypeToSeaTunnelType(keyValueType[0], name + ".key"),
                        turnColumnTypeToSeaTunnelType(keyValueType[1], name + ".value"));
        builder.dataType(mapType);
    }

    private SeaTunnelDataType turnColumnTypeToSeaTunnelType(String columnType, String columnName) {
        BasicTypeDefine keyBasicTypeDefine =
                BasicTypeDefine.builder().columnType(columnType).name(columnName).build();
        if (columnType.toUpperCase(Locale.ROOT).startsWith(DORIS_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnType);
            keyBasicTypeDefine.setPrecision(Long.valueOf(precisionAndScale[0]));
            keyBasicTypeDefine.setScale(precisionAndScale[1]);
        }
        Column column = convert(keyBasicTypeDefine);
        return column.getDataType();
    }

    private void convertArray(
            String columnType, PhysicalColumn.PhysicalColumnBuilder builder, String name) {
        String columnInterType = extractArrayType(columnType);
        if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_BOOLEAN_INTER)) {
            builder.dataType(ArrayType.BOOLEAN_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_TINYINT_INTER)) {
            builder.dataType(ArrayType.BYTE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_SMALLINT_INTER)) {
            builder.dataType(ArrayType.SHORT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_INT_INTER)) {
            builder.dataType(ArrayType.INT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_BIGINT_INTER)) {
            builder.dataType(ArrayType.LONG_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_FLOAT)) {
            builder.dataType(ArrayType.FLOAT_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_DOUBLE)) {
            builder.dataType(ArrayType.DOUBLE_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith("CHAR")
                || columnInterType.toUpperCase(Locale.ROOT).startsWith("VARCHAR")
                || columnInterType.equalsIgnoreCase(DORIS_STRING)) {
            builder.dataType(ArrayType.STRING_ARRAY_TYPE);
        } else if (columnInterType.toUpperCase(Locale.ROOT).startsWith(DORIS_ARRAY_DECIMAL_PRE)) {
            int[] precisionAndScale = getPrecisionAndScale(columnInterType);
            DecimalArrayType decimalArray =
                    new DecimalArrayType(
                            new DecimalType(precisionAndScale[0], precisionAndScale[1]));
            builder.dataType(decimalArray);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_DATE_INTER)
                || columnInterType.equalsIgnoreCase(DORIS_ARRAY_DATEV2_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_ARRAY_DATETIME_INTER)
                || columnInterType.equalsIgnoreCase(DORIS_ARRAY_DATETIMEV2_INTER)) {
            builder.dataType(ArrayType.LOCAL_DATE_TIME_ARRAY_TYPE);
        } else if (columnInterType.equalsIgnoreCase(DORIS_LARGEINT)) {
            DecimalArrayType decimalArray = new DecimalArrayType(new DecimalType(20, 0));
            builder.dataType(decimalArray);
        } else {
            throw CommonError.convertToSeaTunnelTypeError(IDENTIFIER, columnType, name);
        }
    }

    private static String extractArrayType(String input) {
        Pattern pattern = Pattern.compile("<(.*?)>");
        Matcher matcher = pattern.matcher(input);

        return matcher.find() ? matcher.group(1) : "";
    }

    private static String[] extractMapKeyValueType(String input) {
        String[] result = new String[2];
        input = input.replaceAll("map<", "").replaceAll("MAP<", "").replaceAll(">", "");
        String[] split = input.split(",");
        if (split.length == 4) {
            // decimal(10,2),decimal(10,2)
            result[0] = split[0] + "," + split[1];
            result[1] = split[2] + "," + split[3];
        } else if (split.length == 3) {
            // decimal(10,2), date
            // decimal(10, 2), varchar(20)
            if (split[0].indexOf("(") != -1 && split[1].indexOf(")") != -1) {
                result[0] = split[0] + "," + split[1];
                result[1] = split[2];
            } else if (split[1].indexOf("(") != -1 && split[2].indexOf(")") != -1) {
                // date, decimal(10, 2)
                // varchar(20), decimal(10, 2)
                result[0] = split[0];
                result[1] = split[1] + "," + split[2];
            } else {
                return null;
            }
        } else if (split.length == 2) {
            result[0] = split[0];
            result[1] = split[1];
        } else {
            return null;
        }
        return result;
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder = getBasicTypeDefineBuilder(column);

        switch (column.getDataType().getSqlType()) {
            case STRING:
                reconvertString(column, builder);
                break;
            case DATE:
                builder.columnType(DORIS_DATE);
                builder.dataType(DORIS_DATE);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() >= 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(String.format("%s(%s)", DORIS_DATETIME, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(String.format("%s(%s)", DORIS_DATETIME, MAX_DATETIME_SCALE));
                    builder.scale(MAX_DATETIME_SCALE);
                }
                builder.dataType(DORIS_DATETIME);
                break;
            case MAP:
                reconvertMap(column, builder);
                break;
            default:
                super.sampleReconvert(column, builder);
        }
        return builder.build();
    }

    private void reconvertMap(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        MapType dataType = (MapType) column.getDataType();
        SeaTunnelDataType keyType = dataType.getKeyType();
        SeaTunnelDataType valueType = dataType.getValueType();
        Column keyColumn =
                PhysicalColumn.of(
                        column.getName() + ".key",
                        (SeaTunnelDataType<?>) keyType,
                        (Long) null,
                        true,
                        null,
                        null);
        String keyColumnType = reconvert(keyColumn).getColumnType();

        Column valueColumn =
                PhysicalColumn.of(
                        column.getName() + ".value",
                        (SeaTunnelDataType<?>) valueType,
                        (Long) null,
                        true,
                        null,
                        null);
        String valueColumnType = reconvert(valueColumn).getColumnType();

        builder.dataType(String.format(DORIS_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
        builder.columnType(String.format(DORIS_MAP_COLUMN_TYPE, keyColumnType, valueColumnType));
    }

    private void reconvertString(Column column, BasicTypeDefine.BasicTypeDefineBuilder builder) {
        // source is doris too.
        if (column.getSourceType() != null && column.getSourceType().equalsIgnoreCase(DORIS_JSON)) {
            // Compatible with Doris 1.x and Doris 2.x versions
            builder.columnType(DORIS_JSON);
            builder.dataType(DORIS_JSON);
            return;
        }

        super.sampleReconvertString(column, builder);
    }
}
