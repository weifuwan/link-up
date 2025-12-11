package org.apache.cockpit.connectors.sqlserver.catalog;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.cockpit.connectors.api.catalog.Column;
import org.apache.cockpit.connectors.api.catalog.DataTypeConvertor;
import org.apache.cockpit.connectors.api.catalog.PhysicalColumn;
import org.apache.cockpit.connectors.api.jdbc.converter.BasicTypeDefine;
import org.apache.cockpit.connectors.api.jdbc.dialect.DatabaseIdentifier;
import org.apache.cockpit.connectors.api.type.SeaTunnelDataType;
import org.apache.cockpit.connectors.sqlserver.dialect.SqlServerTypeConverter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Map;

@Deprecated
@AutoService(DataTypeConvertor.class)
public class SqlServerDataTypeConvertor implements DataTypeConvertor<SqlServerType> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final String LENGTH = "length";
    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, @NonNull String connectorDataType) {
        Pair<SqlServerType, Map<String, Object>> sqlServerType =
                SqlServerType.parse(connectorDataType);
        return toSeaTunnelType(field, sqlServerType.getLeft(), sqlServerType.getRight());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field,
            @NonNull SqlServerType connectorDataType,
            Map<String, Object> dataTypeProperties) {
        int precision =
                Integer.parseInt(
                        dataTypeProperties.getOrDefault(PRECISION, DEFAULT_PRECISION).toString());
        long length = Long.parseLong(dataTypeProperties.getOrDefault(LENGTH, 0).toString());
        int scale = (int) dataTypeProperties.getOrDefault(SCALE, DEFAULT_SCALE);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .columnType(connectorDataType.getSqlTypeName())
                        .dataType(connectorDataType.getSqlTypeName())
                        .length(length)
                        .precision((long) precision)
                        .scale(scale)
                        .build();

        return SqlServerTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public SqlServerType toConnectorType(
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

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        return SqlServerType.parse(typeDefine.getColumnType()).getLeft();
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.SQLSERVER;
    }
}
