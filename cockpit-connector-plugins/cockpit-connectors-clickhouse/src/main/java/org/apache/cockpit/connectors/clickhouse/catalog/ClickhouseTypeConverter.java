package org.apache.cockpit.connectors.clickhouse.catalog;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.catalog.exception.CommonError;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeConverter;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.converter.TypeConverter;
import org.apache.cockpit.connectors.api.type.ArrayType;
import org.apache.cockpit.connectors.api.type.DecimalType;
import org.apache.cockpit.connectors.api.type.MapType;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.clickhouse.config.ClickhouseType;

@Slf4j
@AutoService(TypeConverter.class)
public class ClickhouseTypeConverter
        implements BasicTypeConverter<BasicTypeDefine<ClickhouseType>> {
    public static final ClickhouseTypeConverter INSTANCE = new ClickhouseTypeConverter();
    public static final Integer MAX_DATETIME_SCALE = 9;
    public static final String IDENTIFIER = "Clickhouse";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Column convert(BasicTypeDefine<ClickhouseType> typeDefine) {
        throw new UnsupportedOperationException("Unsupported operation");
    }

    @Override
    public BasicTypeDefine<ClickhouseType> reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());

        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(ClickhouseType.BOOLEAN);
                builder.dataType(ClickhouseType.BOOLEAN);
                break;
            case TINYINT:
                builder.columnType(ClickhouseType.TINYINT);
                builder.dataType(ClickhouseType.TINYINT);
                break;
            case SMALLINT:
                builder.columnType(ClickhouseType.SMALLINT);
                builder.dataType(ClickhouseType.SMALLINT);
                break;
            case INT:
                builder.columnType(ClickhouseType.INT);
                builder.dataType(ClickhouseType.INT);
                break;
            case BIGINT:
                builder.columnType(ClickhouseType.BIGINT);
                builder.dataType(ClickhouseType.BIGINT);
                break;
            case FLOAT:
                builder.columnType(ClickhouseType.FLOAT);
                builder.dataType(ClickhouseType.FLOAT);
                break;
            case DOUBLE:
                builder.columnType(ClickhouseType.DOUBLE);
                builder.dataType(ClickhouseType.DOUBLE);
                break;
            case DATE:
                builder.columnType(ClickhouseType.DATE);
                builder.dataType(ClickhouseType.DATE);
                break;
            case TIME:
            case STRING:
                builder.columnType(ClickhouseType.STRING);
                builder.dataType(ClickhouseType.STRING);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                builder.columnType(
                        String.format(
                                "%s(%s, %s)",
                                ClickhouseType.DECIMAL,
                                decimalType.getPrecision(),
                                decimalType.getScale()));
                builder.dataType(ClickhouseType.DECIMAL);
                break;
            case TIMESTAMP:
                if (column.getScale() != null
                        && column.getScale() > 0
                        && column.getScale() <= MAX_DATETIME_SCALE) {
                    builder.columnType(
                            String.format("%s(%s)", ClickhouseType.DateTime64, column.getScale()));
                    builder.scale(column.getScale());
                } else {
                    builder.columnType(String.format("%s(%s)", ClickhouseType.DateTime64, 0));
                    builder.scale(0);
                }
                builder.dataType(ClickhouseType.DateTime64);
                break;
            case MAP:
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

                builder.dataType(ClickhouseType.MAP);
                builder.columnType(
                        String.format(
                                "%s(%s, %s)", ClickhouseType.MAP, keyColumnType, valueColumnType));
                break;
            case ARRAY:
                SeaTunnelDataType<?> arrayDataType = column.getDataType();
                SeaTunnelDataType elementType = null;
                if (arrayDataType instanceof ArrayType) {
                    ArrayType arrayType = (ArrayType) arrayDataType;
                    elementType = arrayType.getElementType();
                }

                Column arrayKeyColumn =
                        PhysicalColumn.of(
                                column.getName() + ".key",
                                (SeaTunnelDataType<?>) elementType,
                                (Long) null,
                                true,
                                null,
                                null);
                String arrayKeyColumnType = reconvert(arrayKeyColumn).getColumnType();
                builder.dataType(ClickhouseType.ARRAY);
                builder.columnType(
                        String.format("%s(%s)", ClickhouseType.ARRAY, arrayKeyColumnType));
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        IDENTIFIER, column.getDataType().getSqlType().name(), column.getName());
        }
        return builder.build();
    }
}
