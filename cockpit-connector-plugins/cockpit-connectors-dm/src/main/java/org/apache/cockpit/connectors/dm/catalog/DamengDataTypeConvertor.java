package org.apache.cockpit.connectors.dm.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.DataTypeConvertor;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.dm.dialect.DmdbTypeConverter;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.Map;

import static org.apache.cockpit.connectors.dm.dialect.DmdbTypeConverter.*;


/**
 * @deprecated instead by {@link DmdbTypeConverter}
 */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class DamengDataTypeConvertor implements DataTypeConvertor<String> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String dataType) {
        return toSeaTunnelType(field, dataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String dataType, Map<String, Object> properties) {
        Integer precision = null;
        Integer scale = null;
        switch (dataType.toUpperCase()) {
            case DM_NUMERIC:
            case DM_NUMBER:
            case DM_DECIMAL:
            case DM_DEC:
                precision = MapUtils.getInteger(properties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(properties, SCALE, DEFAULT_SCALE);
                break;
            default:
                break;
        }
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .columnType(dataType)
                        .dataType(dataType)
                        .length(precision == null ? null : precision.longValue())
                        .precision(precision == null ? null : precision.longValue())
                        .scale(scale)
                        .build();

        return DmdbTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field, SeaTunnelDataType<?> dataType, Map<String, Object> properties) {
        Long precision = MapUtils.getLong(properties, PRECISION);
        Integer scale = MapUtils.getInteger(properties, SCALE);
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(dataType)
                        .columnLength(precision)
                        .scale(scale)
                        .nullable(true)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }
}
