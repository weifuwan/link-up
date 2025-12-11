package org.apache.cockpit.connectors.mysql.catalog;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.mysql.cj.MysqlType;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.DataTypeConvertor;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.mysql.dialect.MySqlTypeConverter;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/** @deprecated instead by {@link MySqlTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class MysqlDataTypeConvertor implements DataTypeConvertor<MysqlType> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";

    public static final Integer DEFAULT_PRECISION = 10;

    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        MysqlType mysqlType = MysqlType.getByName(connectorDataType);
        Map<String, Object> dataTypeProperties;
        switch (mysqlType) {
            case BIGINT_UNSIGNED:
            case DECIMAL:
            case DECIMAL_UNSIGNED:
            case BIT:
                // parse precision and scale
                int left = connectorDataType.indexOf("(");
                int right = connectorDataType.indexOf(")");
                int precision = DEFAULT_PRECISION;
                int scale = DEFAULT_SCALE;
                if (left != -1 && right != -1) {
                    String[] precisionAndScale =
                            connectorDataType.substring(left + 1, right).split(",");
                    if (precisionAndScale.length == 2) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                        scale = Integer.parseInt(precisionAndScale[1]);
                    } else if (precisionAndScale.length == 1) {
                        precision = Integer.parseInt(precisionAndScale[0]);
                    }
                }
                dataTypeProperties = ImmutableMap.of(PRECISION, precision, SCALE, scale);
                break;
            default:
                dataTypeProperties = Collections.emptyMap();
                break;
        }
        return toSeaTunnelType(field, mysqlType, dataTypeProperties);
    }

    // todo: It's better to wrapper MysqlType to a pojo in ST, since MysqlType doesn't contains
    // properties.
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, MysqlType mysqlType, Map<String, Object> dataTypeProperties) {
        checkNotNull(mysqlType, "mysqlType can not be null");

        Long precision = MapUtils.getLong(dataTypeProperties, PRECISION);
        Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);
        BasicTypeDefine<MysqlType> typeDefine =
                BasicTypeDefine.<MysqlType>builder()
                        .name(field)
                        .nativeType(mysqlType)
                        .dataType(mysqlType.getName())
                        .columnType(mysqlType.getName())
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();

        return MySqlTypeConverter.DEFAULT_INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public MysqlType toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {

        Long precision = MapUtils.getLong(dataTypeProperties, PRECISION);
        Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(seaTunnelDataType)
                        .columnLength(precision)
                        .scale(scale)
                        .nullable(true)
                        .build();

        BasicTypeDefine<MysqlType> typeDefine =
                MySqlTypeConverter.DEFAULT_INSTANCE.reconvert(column);
        return typeDefine.getNativeType();
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.MYSQL;
    }
}
