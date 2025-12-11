package org.apache.cockpit.connectors.oracle.catalog;

import com.google.auto.service.AutoService;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.DataTypeConvertor;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.oracle.dialect.OracleTypeConverter;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;


/** @deprecated instead by {@link OracleTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class OracleDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Long DEFAULT_PRECISION = 38L;
    public static final Integer DEFAULT_SCALE = 18;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "Oracle Type cannot be null");

        Long precision = null;
        Integer scale = null;
        switch (connectorDataType) {
            case OracleTypeConverter.ORACLE_NUMBER:
                precision = MapUtils.getLong(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                break;
            default:
                break;
        }


        BasicTypeDefine<Object> typeDefine = BasicTypeDefine.builder()
                .name(field)
                .columnType(connectorDataType)
                .dataType(normalizeTimestamp(connectorDataType))
                .length(precision)
                .precision(precision)
                .scale(scale)
                .build();

        return OracleTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");

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

        BasicTypeDefine<Object> typeDefine = OracleTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }

    public static String normalizeTimestamp(String oracleType) {
        // Create a pattern to match TIMESTAMP followed by an optional (0-9)
        String pattern = "^TIMESTAMP(\\([0-9]\\))?$";
        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(oracleType);
        if (m.find()) {
            return "TIMESTAMP";
        } else {
            return oracleType;
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.ORACLE;
    }
}
